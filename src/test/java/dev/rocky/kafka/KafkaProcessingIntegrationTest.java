package dev.rocky.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties = {
    "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
    "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer"
})
class KafkaProcessingIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("events").withUsername("events").withPassword("events");

    @Container
    static final KafkaContainer KAFKA =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @TestConfiguration
    static class Topics {
        @Bean NewTopic eventsTopic() { return TopicBuilder.name("events").partitions(1).replicas(1).build(); }
        @Bean NewTopic deadLetterTopic() { return TopicBuilder.name("events.DLT").partitions(1).replicas(1).build(); }
    }

    @Autowired KafkaTemplate<String, String> template;
    @Autowired JdbcTemplate jdbc;

    @Test
    void listenerPersistsEventAndSkipsRedelivery() throws Exception {
        String id = UUID.randomUUID().toString();
        template.send("events", id, "original").get(15, TimeUnit.SECONDS);
        awaitStored(id);
        template.send("events", id, "duplicate").get(15, TimeUnit.SECONDS);
        Thread.sleep(1200);
        assertEquals("original", jdbc.queryForObject(
            "SELECT payload FROM business_events WHERE event_id = ?", String.class, id));
        assertEquals(1, jdbc.queryForObject(
            "SELECT COUNT(*) FROM processed_events WHERE event_id = ?", Integer.class, id));
    }

    @Test
    void invalidEventIsPublishedToDeadLetterTopicAfterRetries() throws Exception {
        Map<String, Object> settings = new HashMap<>();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID());
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings)) {
            consumer.subscribe(Collections.singletonList("events.DLT"));
            template.send("events", "", "invalid-payload").get(15, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(50);
            boolean recovered = false;
            while (System.nanoTime() < deadline && !recovered) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    if ("invalid-payload".equals(record.value())) {
                        assertEquals("", record.key());
                        recovered = true;
                        break;
                    }
                }
            }
            assertTrue(recovered, "Expected invalid event to reach events.DLT after retry exhaustion");
        }
    }

    private void awaitStored(String id) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (System.nanoTime() < deadline) {
            Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM business_events WHERE event_id = ?", Integer.class, id);
            if (count != null && count == 1) return;
            Thread.sleep(200);
        }
        fail("Kafka event was not persisted within 25 seconds");
    }
}
