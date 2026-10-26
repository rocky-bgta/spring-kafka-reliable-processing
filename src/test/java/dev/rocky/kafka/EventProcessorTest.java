package dev.rocky.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventProcessorTest {
    @Test void newEventWritesClaimAndBusinessRecord() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(contains("ON CONFLICT"), eq("event-1"))).thenReturn(1);
        assertTrue(new EventProcessor(jdbc).process("event-1", "payload"));
        var order = inOrder(jdbc);
        order.verify(jdbc).update(contains("ON CONFLICT"), eq("event-1"));
        order.verify(jdbc).update(contains("INSERT INTO business_events"), eq("event-1"), eq("payload"));
    }

    @Test void duplicateEventIsSkippedWithoutBusinessWrite() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(contains("ON CONFLICT"), eq("event-1"))).thenReturn(0);
        assertFalse(new EventProcessor(jdbc).process("event-1", "payload"));
        verify(jdbc, never()).update(contains("INSERT INTO business_events"), any(), any());
    }

    @Test void invalidInputIsRejectedBeforeDatabaseWrite() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        EventProcessor processor = new EventProcessor(jdbc);
        assertThrows(IllegalArgumentException.class, () -> processor.process(" ", "payload"));
        assertThrows(IllegalArgumentException.class, () -> processor.process("event", null));
        verifyNoInteractions(jdbc);
    }
}
