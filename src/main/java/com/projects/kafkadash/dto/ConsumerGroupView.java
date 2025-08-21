package com.projects.kafkadash.dto;

import java.time.Instant;

public record ConsumerGroupView(
        String groupId,
        String status,
        Long lag,
        Long lastCommittedOffset,
        Instant lastCommitTime
) {}
