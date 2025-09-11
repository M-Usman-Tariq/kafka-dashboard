package com.projects.kafkadash.dto;

import java.time.Instant;

public record ConsumerGroupView(
        String clientName,
        String subscriptionName,
        String runningState,
        String syncState,
        Long lag,
        Long lastCommittedOffset,
        Instant lastCommitTime
) {}
