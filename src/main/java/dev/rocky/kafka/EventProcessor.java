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
     * Claims an event atomically in PostgreSQL. The unique key prevents
     * concurrent consumers from applying the same event twice.
     * Business writes must be performed inside this transaction after a claim.
     *
     * @return true for a new event, false when already processed.
     */
    @Transactional
    public boolean process(String eventId, String payload) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId is required");
        }
        int inserted = jdbc.update(
            "INSERT INTO processed_events(event_id) VALUES (?) ON CONFLICT (event_id) DO NOTHING",
            eventId
        );
        if (inserted == 0) {
            return false;
        }
        // Add the actual domain update here, within this transaction.
        // An exception rolls back the claim so Kafka can safely retry.
        return true;
    }
}
