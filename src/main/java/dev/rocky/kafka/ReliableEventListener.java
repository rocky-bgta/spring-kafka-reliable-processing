package dev.rocky.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class ReliableEventListener {
    private final EventProcessor processor;

    public ReliableEventListener(EventProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(topics = "${app.kafka.topic}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        // The record key is the idempotency key; production schemas should validate it.
        processor.process(record.key(), record.value());
        // Transactional service call has returned after commit; only then acknowledge.
        acknowledgment.acknowledge();
    }
}
