package com.projects.kafkadash.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;

@Data
@AllArgsConstructor
public class CommitInfo {
    long offset;
    Instant timestamp;
}