package com.projects.kafkadash.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "topic_stats")
@Getter
@Setter
public class TopicStats {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String topicName;

    private long messageCount;
    private long lastOffset;
    private Instant lastPublishedAt;
    @Column(nullable = false)
    private Instant refreshTime;
}
