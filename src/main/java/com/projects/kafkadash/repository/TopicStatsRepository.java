package com.projects.kafkadash.repository;

import com.projects.kafkadash.entity.TopicStats;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TopicStatsRepository extends JpaRepository<TopicStats, Long> {
    Optional<TopicStats> findFirstByTopicNameOrderByRefreshTimeDesc(String topicName);

}
