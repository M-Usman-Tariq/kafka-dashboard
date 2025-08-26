package com.projects.kafkadash.service;

import com.projects.kafkadash.entity.Client;
import com.projects.kafkadash.entity.ConsumerGroupStats;
import com.projects.kafkadash.entity.TopicStats;
import com.projects.kafkadash.exception.GlobalExceptionHandler;
import com.projects.kafkadash.repository.ClientRepository;
import com.projects.kafkadash.repository.ConsumerGroupStatsRepository;
import com.projects.kafkadash.repository.TopicStatsRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.scheduling.annotation.Scheduled;

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
    Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);


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

    //@Scheduled(fixedDelayString = "${app.refresh-ms:300000}")
    @Transactional
    public void refreshAll() {
        Instant now = Instant.now();

        // Compute topic stats
        try {
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

            Instant lastPublished = null;

            if(totalMsgs > 0) {
                lastPublished = probeLastPublishedTimestamp(partitions, latest);
            }
            TopicStats ts = new TopicStats();
            ts.setTopicName(topicName);
            ts.setMessageCount(totalMsgs);
            ts.setLastOffset(lastOffset);
            ts.setLastPublishedAt(lastPublished);
            ts.setRefreshTime(now);
            topicRepo.save(ts);

            // For each client group from DB
            List<Client> all = clients.findAll();
            for (Client c : all) {
                String group = c.getConsumerGroupName();
                ConsumerGroupStats row = buildGroupStats(group, partitions, latest, now);
                cgRepo.save(row);
            }

        }
        catch (ExecutionException executionException) {
            if(Objects.nonNull(executionException.getCause())){
                var innerException = executionException.getCause();

                if(innerException instanceof UnknownTopicOrPartitionException){
                    logger.error("Topic '{}' not found in kafka.", topicName);
                }
            }
            else {
                logger.error(executionException.getMessage(), executionException);
            }
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private List<TopicPartition> getPartitions(String topic) throws ExecutionException, InterruptedException {
        Map<String, TopicDescription> desc = admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get();
        return desc.get(topic).partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());
    }

    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsets(List<TopicPartition> tps, OffsetSpec spec) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> req = new HashMap<>();
        for (TopicPartition tp : tps) req.put(tp, spec);
        return admin.listOffsets(req).all().get();
    }

    private Instant probeLastPublishedTimestamp(List<TopicPartition> tps, Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest) {
        // Peek the last record timestamp per partition; return max across partitions
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "metrics-probe");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        Instant maxTs = null;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            for (TopicPartition tp : tps) {
                long hi = latest.get(tp).offset();
                if (hi <= 0) continue;
                consumer.assign(Collections.singleton(tp));
                consumer.seek(tp, hi - 1);
                var recs = consumer.poll(Duration.ofMillis(2000));
                if (!recs.isEmpty()) {
                    var ts = Instant.ofEpochMilli(recs.iterator().next().timestamp());
                    if (maxTs == null || ts.isAfter(maxTs)) maxTs = ts;
                }
            }
        } catch (Exception e) {
            // ignore, return null
        }
        return maxTs;
    }

    private ConsumerGroupStats buildGroupStats(String group,
                                               List<TopicPartition> topicPartitions,
                                               Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest,
                                               Instant now) throws ExecutionException, InterruptedException {
        // Try to fetch committed offsets for group
        Map<TopicPartition, OffsetAndMetadata> committed = null;
        try {
            committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        } catch (Exception e) {
            committed = null;
        }

        ConsumerGroupStats row = new ConsumerGroupStats();
        row.setConsumerGroupName(group);
        row.setTopicName(topicName);
        row.setRefreshTime(now);

        // Find previous snapshot
        var prevOpt = cgRepo.findFirstByConsumerGroupNameAndTopicNameOrderByRefreshTimeDesc(group, topicName);
        Long prevCommittedSum = prevOpt.map(ConsumerGroupStats::getLastCommittedOffset).orElse(null);
        Instant prevCommitTime = prevOpt.map(ConsumerGroupStats::getLastCommitTime).orElse(null);

        if (committed == null || committed.isEmpty()) {
            // INACTIVE: use previous lastCommitTime if exists else null
            row.setStatus("INACTIVE");
            row.setLag(null);
            row.setLastCommittedOffset(prevCommittedSum);
            row.setLastCommitTime(prevCommitTime);
            return row;
        }

        // Filter to topic partitions only
        Map<TopicPartition, OffsetAndMetadata> topicCommitted = committed.entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topicName))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        long lagSum = 0L;
        long committedSum = 0L;
        for (TopicPartition tp : topicPartitions) {
            long hi = latest.get(tp).offset();
            long comm = topicCommitted.getOrDefault(tp, new OffsetAndMetadata(0L)).offset();
            committedSum += comm;
            lagSum += Math.max(hi - comm, 0);
        }

        row.setStatus("ACTIVE");
        row.setLag(lagSum);
        row.setLastCommittedOffset(committedSum);

        // Last commit time heuristic: if committedSum increased since last snapshot => update time to now
        if (prevCommittedSum == null || committedSum > prevCommittedSum) {
            row.setLastCommitTime(now);
        } else {
            // If no change, keep previous value (could be null)
            row.setLastCommitTime(prevCommitTime);
        }

        return row;
    }
}
