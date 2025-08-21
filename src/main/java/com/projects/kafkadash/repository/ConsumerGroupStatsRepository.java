package com.projects.kafkadash.repository;

import com.projects.kafkadash.entity.ConsumerGroupStats;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ConsumerGroupStatsRepository extends JpaRepository<ConsumerGroupStats, Long> {
    Optional<ConsumerGroupStats> findFirstByConsumerGroupNameAndTopicNameOrderByRefreshTimeDesc(String consumerGroupName, String topicName);
}
