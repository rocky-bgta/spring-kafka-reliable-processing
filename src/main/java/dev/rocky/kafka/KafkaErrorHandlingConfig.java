package dev.rocky.kafka;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaErrorHandlingConfig {
    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            template,
            (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        );
        ExponentialBackOff backoff = new ExponentialBackOff(1000L, 2.0);
        backoff.setMaxInterval(8000L);
        backoff.setMaxElapsedTime(15000L);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backoff);
        handler.setCommitRecovered(true);
        return handler;
    }
}
