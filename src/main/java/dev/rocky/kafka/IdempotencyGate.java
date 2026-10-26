package dev.rocky.kafka;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory demonstration only: use a unique DB constraint in production. */
public final class IdempotencyGate {
    private final Set<String> processed = ConcurrentHashMap.newKeySet();

    public boolean claim(String eventId) {
        if (eventId == null || eventId.isBlank()) throw new IllegalArgumentException("eventId is required");
        return processed.add(eventId);
    }
}
