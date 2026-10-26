package dev.rocky.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class KafkaErrorHandlingConfigTest {
    @Test
    void configuresRecoveringErrorHandler() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<Object, Object> template = mock(KafkaTemplate.class);
        CommonErrorHandler handler = new KafkaErrorHandlingConfig().kafkaErrorHandler(template);
        assertInstanceOf(DefaultErrorHandler.class, handler);
        assertTrue(handler.isAckAfterHandle());
    }
}
