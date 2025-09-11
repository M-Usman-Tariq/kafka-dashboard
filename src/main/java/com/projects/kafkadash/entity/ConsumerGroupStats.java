package com.projects.kafkadash.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "consumer_group_stats")
@Getter
@Setter
public class ConsumerGroupStats {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false)
    private String consumerGroupName;
    @Column(nullable = false)
    private String topicName;
    @Column(nullable = false)
    private String clientName;
    @Column(nullable = false)
    private Long lag;
    @Column(nullable = false)
    private Long lastCommittedOffset;
    @Column(nullable = false)
    private Instant lastCommitTime;
    @Column(nullable = false)
    private String runningState; // RUNNING / IDLE
    @Column(nullable = false)
    private String syncStatus;   // ACTIVE / LAGGING / INACTIVE

    @Column(nullable = false)
    private Instant refreshTime;
}
