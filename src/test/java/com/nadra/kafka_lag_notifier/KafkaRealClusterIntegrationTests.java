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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = KafkaDashboardApplication.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles("test")
class KafkaRealClusterIntegrationTests {

    private static final String TOPIC1 = "citizen-status";
    private static final String GROUP1 = "sink-connector-bisp";
    private static final String GROUP2 = "sink-connector-askari";
    private static final String GROUP3 = "sink-connector-fia";

    private static final String BOOSTRAP_SERVERS = "172.25.240.68:9092";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaMetricsService kafkaMetricsService;

    private TopicStatsService topicStatsService;

    @Autowired
    private ClientRepository clientRepo;
    @Autowired
    private TopicStatsRepository topicRepo;
    @Autowired
    private ConsumerGroupStatsRepository cgRepo;

    @BeforeAll
    void setupCluster() {
        AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVERS)
        );

        try {
            admin.deleteTopics(List.of(TOPIC1)).all().get();

            // wait a moment for deletion
            Thread.sleep(5000);
            // recreate topic
            admin.createTopics(List.of(new NewTopic(TOPIC1, 1, (short) 1))).all().get();
        } catch (Exception e) {
            System.out.println(e);
        }



        kafkaMetricsService = new KafkaMetricsService(
                admin, clientRepo, topicRepo, cgRepo, BOOSTRAP_SERVERS, TOPIC1
        );

        topicStatsService = new TopicStatsService(topicRepo, cgRepo, clientRepo);
        // make sure topic exists
        System.out.println("==> Running tests against real Kafka cluster <==");
    }

    @BeforeEach
    void cleanDb() {
        cgRepo.deleteAll();
        topicRepo.deleteAll();
        clientRepo.deleteAll();
    }

    @AfterEach
    void resetKafka() throws Exception {
        kafkaTemplate.flush();
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVERS
        ))) {
            // delete topic
            admin.deleteTopics(List.of(TOPIC1)).all().get();

            // wait a moment for deletion
            Thread.sleep(5000);

            // recreate topic
            admin.createTopics(List.of(new NewTopic(TOPIC1, 1, (short) 1))).all().get();
        }

        System.out.println("Open http://localhost:8080 to view dashboard");
        Thread.sleep(30000); // keep alive for 5 minutes
    }

    @Test
    @Order(1)
    void testSingleClientCaughtUp() {
        createClient("bisp", GROUP1, TOPIC1);
        kafkaTemplate.send(TOPIC1, "msg1");
        kafkaTemplate.send(TOPIC1, "msg2");
        kafkaTemplate.flush();

        consumeMessages(GROUP1, TOPIC1, 2);

        kafkaMetricsService.refreshAll();
        TopicView view = topicStatsService.stats(TOPIC1);

        var cgView = view.consumers().stream()
                .filter(c -> GROUP1.equals(c.subscriptionName()))
                .findFirst().orElseThrow();

        System.out.printf("==> Caught up check: group=%s lag=%d status=%s%n",
                cgView.subscriptionName(), cgView.lag(), cgView.syncState());

        assertThat(cgView.lag()).isEqualTo(0);
        assertThat(cgView.runningState()).isEqualTo("RUNNING");   // no active members
        assertThat(cgView.syncState()).isEqualTo("SYNCED");

    }

    @Test
    @Order(2)
    void testSingleClientLagging() {
        createClient("askari", GROUP2, TOPIC1);
        kafkaTemplate.send(TOPIC1, "a");
        kafkaTemplate.send(TOPIC1, "b");
        kafkaTemplate.send(TOPIC1, "c");
        kafkaTemplate.flush();

        // Consume only 1 of 3
        consumeMessages(GROUP2, TOPIC1, 1);

        kafkaMetricsService.refreshAll();
        TopicView view = topicStatsService.stats(TOPIC1);

        var cgView = view.consumers().stream()
                .filter(c -> GROUP2.equals(c.subscriptionName()))
                .findFirst().orElseThrow();

        System.out.printf("==> Lagging check: group=%s lag=%d%n",
                cgView.subscriptionName(), cgView.lag());

        assertThat(cgView.lag()).isGreaterThan(0);
        assertThat(cgView.runningState()).isEqualTo("RUNNING");
        assertThat(cgView.syncState()).isEqualTo("LAGGING");
    }

    @Test
    @Order(3)
    void testMultipleClientsMixed() {
        createClient("bisp", GROUP1, TOPIC1);
        createClient("askari", GROUP2, TOPIC1);

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

        System.out.printf("==> Mixed check: group1 lag=%d, group2 lag=%d%n",
                cg1.lag(), cg2.lag());

        assertThat(cg1.lag()).isEqualTo(0);
        assertThat(cg2.lag()).isGreaterThan(0);

        assertThat(cg1.runningState()).isEqualTo("RUNNING");
        assertThat(cg1.syncState()).isEqualTo("SYNCED");

        assertThat(cg2.runningState()).isEqualTo("RUNNING");
        assertThat(cg2.syncState()).isEqualTo("LAGGING");
    }

    @Test
    @Order(4)
    void testInactiveClient() {
        createClient("fia", GROUP3, TOPIC1);

        kafkaTemplate.send(TOPIC1, "m1");
        kafkaTemplate.send(TOPIC1, "m2");
        kafkaTemplate.flush();

        // no consumer started for GROUP3

        kafkaMetricsService.refreshAll();
        TopicView view = topicStatsService.stats(TOPIC1);

        var cgView = view.consumers().stream().filter(c -> GROUP3.equals(c.subscriptionName())).findFirst().orElseThrow();

        System.out.printf("==> Inactive check: group=%s status=%s%n",
                cgView.subscriptionName(), cgView.syncState());

        assertThat(cgView.runningState()).isEqualTo("IDLE");
        assertThat(cgView.syncState()).isEqualTo("INACTIVE");
    }

    // --------------------
    // Helpers (replace with your impls)
    // --------------------

    private void createClient(String id, String group, String topic) {
        Client c = new Client();
        c.setClientId(id);
        c.setClientName(id.toUpperCase());
        c.setContactName("contacName");
        c.setContactEmail("contact@gmail.com");
        c.setSubscriptionName(group);
        clientRepo.save(c);
    }

    private void consumeMessages(String groupId, String topic, int expectedCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVERS); // <-- inject from config ideally
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // manual commit

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            int consumed = 0;
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

            while (consumed < expectedCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    System.out.printf("Timeout while waiting for messages. Consumed=%d, expected=%d%n", consumed, expectedCount);
                    break;
                }

                for (ConsumerRecord<String, String> rec : records) {
                    consumed++;
                    System.out.printf("Consumed: group=%s, topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                            groupId, rec.topic(), rec.partition(), rec.offset(), rec.key(), rec.value());

                    // Track the last offset for each partition
                    offsetsToCommit.put(
                            new TopicPartition(rec.topic(), rec.partition()),
                            new OffsetAndMetadata(rec.offset() + 1) // commit the "next" offset
                    );

                    if (consumed >= expectedCount) {
                        break; // stop once we've reached the target
                    }
                }
            }

            if (!offsetsToCommit.isEmpty()) {
                consumer.commitSync(offsetsToCommit); // commit exactly up to expectedCount
                System.out.printf("Committed offsets: %s for group=%s%n", offsetsToCommit, groupId);
            }

            System.out.printf("Finished consuming %d messages for group=%s, topic=%s%n",
                    consumed, groupId, topic);
        }
    }


    // --------------------
    // Kafka Beans (optional if already in your app)
    // --------------------
    @EnableKafka
    @Configuration
    static class KafkaTestConfig {
        @Bean
        public ProducerFactory<String, String> producerFactory() {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVERS); // change to real cluster
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }

        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
            Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVERS);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return new DefaultKafkaConsumerFactory<>(config);
        }
    }
}
