package com.projects.kafkadash.dto;

public record GroupTopicPartition(String group, String topic, int partition) {
}
