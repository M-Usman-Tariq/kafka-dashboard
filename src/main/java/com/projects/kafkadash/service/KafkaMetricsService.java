package com.projects.kafkadash.service;

import com.projects.kafkadash.dto.CommitInfo;
import com.projects.kafkadash.dto.GroupTopicPartition;
import com.projects.kafkadash.util.OffsetKey;
import com.projects.kafkadash.entity.Client;
import com.projects.kafkadash.entity.ConsumerGroupStats;
import com.projects.kafkadash.entity.TopicStats;
import com.projects.kafkadash.repository.ClientRepository;
import com.projects.kafkadash.repository.ConsumerGroupStatsRepository;
import com.projects.kafkadash.repository.TopicStatsRepository;
import com.projects.kafkadash.util.OffsetMessageParser;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaMetricsService {

    private final AdminClient admin;
    private final ClientRepository clients;
    private final TopicStatsRepository topicRepo;
    private final ConsumerGroupStatsRepository cgRepo;
    private final String bootstrapServers;
    private final String topicName;
    private static final Logger logger = LoggerFactory.getLogger(KafkaMetricsService.class);

    public KafkaMetricsService(AdminClient admin,
                               ClientRepository clients,
                               TopicStatsRepository topicRepo,
                               ConsumerGroupStatsRepository cgRepo,
                               @Value("${kafka.bootstrap-servers}") String bootstrapServers,
                               @Value("${app.topic-name}") String topicName) {
        this.admin = admin;
        this.clients = clients;
        this.topicRepo = topicRepo;
        this.cgRepo = cgRepo;
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
    }

    /**
     * Runs every X ms — adjust via app.refresh-ms
     */
    @Scheduled(fixedDelayString = "${app.refresh-ms:300000}")
    @Transactional
    public void refreshAll() {
        Instant now = Instant.now();

        try {
            // 1. Collect topic stats
            var partitions = getPartitions(topicName);
            var earliest = listOffsets(partitions, OffsetSpec.earliest());
            var latest = listOffsets(partitions, OffsetSpec.latest());

            long totalMsgs = 0L;
            long lastOffset = 0L;
            for (TopicPartition tp : partitions) {
                long lo = earliest.get(tp).offset();
                long hi = latest.get(tp).offset();
                totalMsgs += Math.max(hi - lo, 0);
                lastOffset = Math.max(lastOffset, hi);
            }

            TopicStats ts = new TopicStats();
            ts.setTopicName(topicName);
            ts.setMessageCount(totalMsgs);
            ts.setLastOffset(lastOffset);
            ts.setLastPublishedAt(Instant.now()); // optional probe
            ts.setRefreshTime(now);
            topicRepo.save(ts);

            // 2. Build dictionary of latest commits from __consumer_offsets
            Map<GroupTopicPartition, CommitInfo> latestCommits = readCommitLog();

            Set<String> runningGroups = admin.listConsumerGroups().all().get()
                    .stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toSet());

            if(latestCommits.isEmpty() && runningGroups.isEmpty())
                return;

            for (Client c : clients.findAll()) {
                ConsumerGroupStats row = buildClientStats(c, partitions, latest, now, latestCommits, runningGroups);
                if(Objects.nonNull(row)) {
                    cgRepo.save(row);
                }
            }

        } catch (ExecutionException executionException) {
            if (Objects.nonNull(executionException.getCause())) {
                if (executionException.getCause() instanceof UnknownTopicOrPartitionException) {
                    logger.error("Topic '{}' not found in Kafka.", topicName);
                }
            } else {
                logger.error(executionException.getMessage(), executionException);
            }
        } catch (Exception e) {
            logger.error("Kafka metrics refresh failed", e);
        }
    }

    // --- Core Helpers ---

    private List<TopicPartition> getPartitions(String topic) throws ExecutionException, InterruptedException {
        Map<String, TopicDescription> desc = admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get();
        return desc.get(topic).partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());
    }

    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsets(
            List<TopicPartition> tps, OffsetSpec spec
    ) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> req = new HashMap<>();
        for (TopicPartition tp : tps) req.put(tp, spec);
        return admin.listOffsets(req).all().get();
    }

    /**
     * Consume __consumer_offsets and keep only latest record per (group, topic, partition).
     */
    private Map<GroupTopicPartition, CommitInfo> readCommitLog() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-commit-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<GroupTopicPartition, CommitInfo> latestCommits = new HashMap<>();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            // subscribe to ALL partitions of __consumer_offsets
            List<PartitionInfo> partitions = consumer.partitionsFor("__consumer_offsets");
            List<TopicPartition> topicPartitions = partitions.stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(topicPartitions);

            boolean more = true;
            while (more) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(10));
                if (records.isEmpty()) {
                    more = false; // stop when no more messages
                }
                for (ConsumerRecord<byte[], byte[]> rec : records) {
                    OffsetKey key = OffsetKey.tryParse(rec.key());
                    if (key == null || rec.value() == null) {
                        continue; // skip non-offset messages
                    }

                    OffsetMessageParser.OffsetAndMetadata committed =
                            OffsetMessageParser.parseOffsetMessageValue(rec.value());

                    if (committed == null) continue;

                    GroupTopicPartition gtp = new GroupTopicPartition(
                            key.getSubscriberGroupName(),
                            key.getTopicName(),
                            key.getPartition()
                    );

                    CommitInfo info = new CommitInfo(
                            committed.offset(),                               // ✅ actual committed offset
                            Instant.ofEpochMilli(committed.commitTimestamp()) // ✅ actual commit timestamp
                    );

                    latestCommits.put(gtp, info);
                }

            }
        } catch (Exception e) {
            logger.error("Error consuming __consumer_offsets", e);
        }

        return latestCommits;
    }


    private ConsumerGroupStats buildClientStats(
            Client client,
            List<TopicPartition> topicPartitions,
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest,
            Instant now,
            Map<GroupTopicPartition, CommitInfo> commits,
            Set<String> runningGroups
    ) {
        String group = client.getSubscriptionName();

        ConsumerGroupStats row = new ConsumerGroupStats();
        row.setConsumerGroupName(group);
        row.setClientName(client.getClientName());
        row.setTopicName(topicName);
        row.setRefreshTime(now);

        // --- Running State ---
        row.setRunningState(runningGroups.contains(group) ? "RUNNING" : "IDLE");

        long totalLag = 0L;
        Instant lastCommitTime = null;
        long lastCommittedOffset = 0L;

        for (TopicPartition tp : topicPartitions) {
            long hi = latest.get(tp).offset();
            GroupTopicPartition gtp = new GroupTopicPartition(group, tp.topic(), tp.partition());
            CommitInfo info = commits.get(gtp);

            long committedOffset = (info != null) ? info.getOffset() : 0L;
            lastCommittedOffset = Math.max(committedOffset, lastCommittedOffset);
            long lag = Math.max(hi - committedOffset, 0);

            totalLag += lag;

            if (info != null && (lastCommitTime == null || info.getTimestamp().isAfter(lastCommitTime))) {
                lastCommitTime = info.getTimestamp();
            }
        }

        // --- Handle missing commit info ---
        if (lastCommitTime == null) {
            // fallback to last stored DB record
            var prevInfo = cgRepo.findFirstByConsumerGroupNameAndTopicNameOrderByRefreshTimeDesc(group, topicName);
            if (prevInfo.stream().isParallel()) {
                var prev = prevInfo.get();
                lastCommitTime = prev.getLastCommitTime();
                lastCommittedOffset = prev.getLastCommittedOffset();
            }
        }

        if (lastCommitTime == null) {
            // nothing known → skip
            return null;
        }

        row.setLag(totalLag);
        row.setLastCommittedOffset(lastCommittedOffset);
        row.setLastCommitTime(lastCommitTime);

        // --- Sync Status ---
        if (Duration.between(lastCommitTime, now).toHours() > 24) {
            row.setSyncStatus("INACTIVE");
        } else if (totalLag > 0) {
            row.setSyncStatus("LAGGING");
        } else {
            row.setSyncStatus("SYNCED");
        }

        return row;
    }

}
