package com.projects.kafkadash.service;

import com.projects.kafkadash.dto.ConsumerGroupView;
import com.projects.kafkadash.dto.TopicView;
import com.projects.kafkadash.entity.TopicStats;
import com.projects.kafkadash.repository.ClientRepository;
import com.projects.kafkadash.repository.ConsumerGroupStatsRepository;
import com.projects.kafkadash.repository.TopicStatsRepository;
import org.springframework.stereotype.Service;

import java.util.Comparator;

@Service
public class TopicStatsService {

    private final TopicStatsRepository topicRepo;
    private final ConsumerGroupStatsRepository cgRepo;
    private final ClientRepository clientRepo;

    public TopicStatsService(TopicStatsRepository topicRepo, ConsumerGroupStatsRepository cgRepo, ClientRepository clientRepo) {
        this.topicRepo = topicRepo;
        this.cgRepo = cgRepo;
        this.clientRepo = clientRepo;
    }

    public TopicView stats(String topic) {
        TopicStats t = topicRepo.findFirstByTopicNameOrderByRefreshTimeDesc(topic)
                .orElseGet(() -> {
                    TopicStats x = new TopicStats();
                    x.setTopicName(topic);
                    x.setMessageCount(0);
                    x.setLastOffset(0);
                    x.setLastPublishedAt(null);
                    x.setRefreshTime(null);
                    return x;
                });

        var consumers = clientRepo.findAll().stream().map(c -> {
                    var latest = cgRepo.findFirstByConsumerGroupNameAndTopicNameOrderByRefreshTimeDesc(c.getSubscriptionName(), topic).orElse(null);
                    if (latest == null) {
                        return new ConsumerGroupView(c.getClientName(), c.getSubscriptionName(), "IDLE","INACTIVE", null, null, null);
                    }
                    return new ConsumerGroupView(latest.getClientName(),
                            latest.getConsumerGroupName(),
                            latest.getRunningState(),
                            latest.getSyncStatus(),
                            latest.getLag(),
                            latest.getLastCommittedOffset(),
                            latest.getLastCommitTime());
                }).sorted(Comparator.comparing(ConsumerGroupView::subscriptionName))
                .toList();

        return new TopicView(topic, t.getMessageCount(), t.getLastOffset(), t.getLastPublishedAt(), t.getRefreshTime(), consumers);
    }
}
