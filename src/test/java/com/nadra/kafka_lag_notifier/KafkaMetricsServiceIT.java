package com.nadra.kafka_lag_notifier;

import com.projects.kafkadash.KafkaDashboardApplication;
import com.projects.kafkadash.dto.TopicView;
import com.projects.kafkadash.entity.Client;
import com.projects.kafkadash.repository.ClientRepository;
import com.projects.kafkadash.repository.ConsumerGroupStatsRepository;
import com.projects.kafkadash.repository.TopicStatsRepository;
import com.projects.kafkadash.service.KafkaMetricsService;
import com.projects.kafkadash.service.TopicStatsService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(classes = KafkaDashboardApplication.class)
class KafkaMetricsServiceIT {

    static final String TOPIC1 = "topic-1";
    static final String GROUP1 = "group-1";
    static final String GROUP2 = "group-2";
    static final String GROUP3 = "group-3";

    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.3")
    );

    @Autowired
    private ClientRepository clientRepo;
    @Autowired
    private TopicStatsRepository topicRepo;
    @Autowired
    private ConsumerGroupStatsRepository cgRepo;

    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaMetricsService kafkaMetricsService;
    private TopicStatsService topicStatsService;

    @BeforeAll
    void startKafka() {
        kafka.start();

        // Producer
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        // AdminClient + Service
        AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
        );
        kafkaMetricsService = new KafkaMetricsService(
                admin, clientRepo, topicRepo, cgRepo, kafka.getBootstrapServers(), TOPIC1
        );

        topicStatsService = new TopicStatsService(topicRepo, cgRepo, clientRepo);
    }

    @BeforeEach
    void cleanDb() {
        cgRepo.deleteAll();
        topicRepo.deleteAll();
        clientRepo.deleteAll();
    }

    @AfterEach
    void resetKafka() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ))) {
            // delete topic
            admin.deleteTopics(List.of(TOPIC1)).all().get();

            // wait a moment for deletion
            Thread.sleep(2000);

            // recreate topic
            admin.createTopics(List.of(new NewTopic(TOPIC1, 1, (short) 1))).all().get();
        }
    }


    private void createClient(String id, String group, String topic) {
        Client c = new Client();
        c.setClientId(id);
        c.setClientName(id);
        c.setContactName("contacName");
        c.setContactEmail("contact@gmail.com");
        c.setSubscriptionName(group);
        clientRepo.save(c);
    }

    private void consumeMessages(String group, String topic, int maxMessages) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // important

        try (Consumer<String, String> consumer =
                     new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
            consumer.subscribe(List.of(topic));

            int count = 0;
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

            while (count < maxMessages) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> rec : recs) {
                    count++;
                    // Track the *next* offset to commit (last consumed + 1)
                    offsetsToCommit.put(
                            new TopicPartition(rec.topic(), rec.partition()),
                            new OffsetAndMetadata(rec.offset() + 1)
                    );
                    if (count >= maxMessages) {
                        break;
                    }
                }
            }

            // Explicitly commit the last offsets consumed for each partition
            if (!offsetsToCommit.isEmpty()) {
                consumer.commitSync(offsetsToCommit);
            }
        }
    }


    @Test
    void testSingleClientCaughtUp() {
        createClient("c1", GROUP1, TOPIC1);
        kafkaTemplate.send(TOPIC1, "msg1");
        kafkaTemplate.send(TOPIC1, "msg2");
        kafkaTemplate.flush();

        consumeMessages(GROUP1, TOPIC1, 2);

        kafkaMetricsService.refreshAll();
        TopicView view = topicStatsService.stats(TOPIC1);

        var cgView = view.consumers().stream().filter(c -> GROUP1.equals(c.subscriptionName())).findFirst().orElseThrow();
        assertThat(cgView.lag()).isEqualTo(0);
        assertThat(cgView.runningState()).isEqualTo("RUNNING");   // no active members
        assertThat(cgView.syncState()).isEqualTo("SYNCED");
    }

    @Test
    void testSingleClientLagging() {
        createClient("c2", GROUP2, TOPIC1);
        kafkaTemplate.send(TOPIC1, "a");
        kafkaTemplate.send(TOPIC1, "b");
        kafkaTemplate.send(TOPIC1, "c");
        kafkaTemplate.flush();

        // Consume only 1 of 3
        consumeMessages(GROUP2, TOPIC1, 1);

        kafkaMetricsService.refreshAll();
        TopicView view = topicStatsService.stats(TOPIC1);

        var cgView = view.consumers().stream().filter(c -> GROUP2.equals(c.subscriptionName())).findFirst().orElseThrow();
        assertThat(cgView.lag()).isGreaterThan(0);
        assertThat(cgView.runningState()).isEqualTo("RUNNING");
        assertThat(cgView.syncState()).isEqualTo("LAGGING");
    }

    @Test
    void testMultipleClientsMixed() {
        createClient("c1", GROUP1, TOPIC1);
        createClient("c2", GROUP2, TOPIC1);

        for (int i = 0; i < 5; i++) {
            kafkaTemplate.send(TOPIC1, "key" + i, "msg-" + i);
        }
        kafkaTemplate.flush();

        consumeMessages(GROUP1, TOPIC1, 5); // caught up
        consumeMessages(GROUP2, TOPIC1, 2); // lagging

        kafkaMetricsService.refreshAll();
        TopicView view = topicStatsService.stats(TOPIC1);

        var cg1 = view.consumers().stream().filter(c -> GROUP1.equals(c.subscriptionName())).findFirst().orElseThrow();
        var cg2 = view.consumers().stream().filter(c -> GROUP2.equals(c.subscriptionName())).findFirst().orElseThrow();

        assertThat(cg1.lag()).isEqualTo(0);
        assertThat(cg2.lag()).isGreaterThan(0);

        assertThat(cg1.runningState()).isEqualTo("RUNNING");
        assertThat(cg1.syncState()).isEqualTo("SYNCED");

        assertThat(cg2.runningState()).isEqualTo("RUNNING");
        assertThat(cg2.syncState()).isEqualTo("LAGGING");
    }

    @Test
    void testInactiveClient() {
        createClient("c3", GROUP3, TOPIC1);

        kafkaTemplate.send(TOPIC1, "m1");
        kafkaTemplate.send(TOPIC1, "m2");
        kafkaTemplate.flush();

        // no consumer started for GROUP3

        kafkaMetricsService.refreshAll();
        TopicView view = topicStatsService.stats(TOPIC1);

        var cgView = view.consumers().stream().filter(c -> GROUP3.equals(c.subscriptionName())).findFirst().orElseThrow();
        assertThat(cgView.runningState()).isEqualTo("IDLE");
        assertThat(cgView.syncState()).isEqualTo("INACTIVE");
    }

    @AfterAll
    static void stopKafka() {
        kafka.stop();
    }
}
