package dev.rocky.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventProcessorTest {
    @Test void newEventIsClaimed() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), eq("event-1"))).thenReturn(1);
        assertTrue(new EventProcessor(jdbc).process("event-1", "payload"));
        verify(jdbc).update(contains("ON CONFLICT"), eq("event-1"));
    }

    @Test void duplicateEventIsSkipped() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), eq("event-1"))).thenReturn(0);
        assertFalse(new EventProcessor(jdbc).process("event-1", "payload"));
    }

    @Test void blankKeyIsRejectedBeforeDatabaseWrite() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        assertThrows(IllegalArgumentException.class,
            () -> new EventProcessor(jdbc).process(" ", "payload"));
        verifyNoInteractions(jdbc);
    }
}
