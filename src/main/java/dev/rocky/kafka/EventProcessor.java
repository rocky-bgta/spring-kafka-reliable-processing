package dev.rocky.kafka;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EventProcessor {
    private final ProcessedEventRepository repository;

    public EventProcessor(ProcessedEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void process(String eventId, String payload) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId is required");
        }
        // Unique database key makes concurrent claims safe across application instances.
        // In a real application, persist the business update in this same transaction.
        repository.saveAndFlush(new ProcessedEvent(eventId));
    }
}
