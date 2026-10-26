package dev.rocky.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ReliableEventListenerTest {
    @Test void acknowledgesOnlyAfterProcessingReturns() {
        EventProcessor processor = mock(EventProcessor.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        ReliableEventListener listener = new ReliableEventListener(processor);
        listener.consume(new ConsumerRecord<>("events", 0, 1L, "id-1", "body"), ack);
        var order = inOrder(processor, ack);
        order.verify(processor).process("id-1", "body");
        order.verify(ack).acknowledge();
    }

    @Test void failureDoesNotAcknowledge() {
        EventProcessor processor = mock(EventProcessor.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        doThrow(new IllegalStateException("db unavailable")).when(processor).process("id-1", "body");
        ReliableEventListener listener = new ReliableEventListener(processor);
        assertThrows(IllegalStateException.class,
            () -> listener.consume(new ConsumerRecord<>("events", 0, 1L, "id-1", "body"), ack));
        verifyNoInteractions(ack);
    }
}
