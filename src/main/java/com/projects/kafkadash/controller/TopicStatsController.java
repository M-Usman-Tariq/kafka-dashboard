package com.projects.kafkadash.controller;

import com.projects.kafkadash.dto.TopicView;
import com.projects.kafkadash.service.TopicStatsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/topics")
public class TopicStatsController {

    private final TopicStatsService topicStatsService;

    public TopicStatsController(TopicStatsService topicStatsService) {
        this.topicStatsService = topicStatsService;
    }

    @GetMapping("/{topic}/stats")
    public TopicView stats(@PathVariable("topic") String topic) {
        return topicStatsService.stats(topic);
    }
}
