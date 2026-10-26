package dev.rocky.kafka;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EventProcessor {
    private final JdbcTemplate jdbc;

    public EventProcessor(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * The idempotency claim and domain write share one database transaction.
     * Kafka redelivery after commit is harmless; failure before commit rolls
     * back both writes, allowing a later delivery to retry.
     */
    @Transactional
    public boolean process(String eventId, String payload) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId is required");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        int inserted = jdbc.update(
            "INSERT INTO processed_events(event_id) VALUES (?) ON CONFLICT (event_id) DO NOTHING",
            eventId
        );
        if (inserted == 0) {
            return false;
        }
        jdbc.update("INSERT INTO business_events(event_id, payload) VALUES (?, ?)", eventId, payload);
        return true;
    }
}
