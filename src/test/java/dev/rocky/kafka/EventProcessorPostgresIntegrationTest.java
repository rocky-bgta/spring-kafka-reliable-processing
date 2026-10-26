package dev.rocky.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class EventProcessorPostgresIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("events")
            .withUsername("events")
            .withPassword("events");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired EventProcessor processor;
    @Autowired JdbcTemplate jdbc;

    @Test
    void firstDeliveryPersistsBusinessWriteAndDuplicateDoesNotChangeIt() {
        String id = "integration-duplicate-event";
        assertTrue(processor.process(id, "first"));
        assertFalse(processor.process(id, "redelivery"));
        Integer count = jdbc.queryForObject(
            "SELECT COUNT(*) FROM processed_events WHERE event_id = ?", Integer.class, id);
        assertEquals(1, count);
        assertEquals("first", jdbc.queryForObject(
            "SELECT payload FROM business_events WHERE event_id = ?", String.class, id));
    }

    @Test
    void invalidEventDoesNotWriteToDatabase() {
        assertThrows(IllegalArgumentException.class, () -> processor.process("", "payload"));
    }
}
