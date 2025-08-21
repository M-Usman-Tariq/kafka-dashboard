package com.projects.kafkadash.dto;

import java.time.Instant;
import java.util.List;

public record TopicView(
        String topic,
        long messageCount,
        long lastOffset,
        Instant lastPublishedAt,
        Instant refreshTime,
        List<ConsumerGroupView> consumers
) {}
