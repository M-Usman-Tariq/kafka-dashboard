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

    @Column(name = "total_lag")
    private Long lag;
    private Long lastCommittedOffset;
    private Instant lastCommitTime;
    @Column(nullable = false)
    private String status; // ACTIVE or INACTIVE
    @Column(nullable = false)
    private Instant refreshTime;
}
