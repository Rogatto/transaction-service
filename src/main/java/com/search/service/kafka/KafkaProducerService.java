package com.search.service.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.search.service.dto.TransactionRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Service
@ConditionalOnProperty(prefix = "kafka", name = "enabled", havingValue = "true")
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final String deadLetterTopic;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper,
                                @Value("${kafka.topic.transactions:transactions}") String topic,
                                @Value("${kafka.topic.dead-letter:transaction-dead-letter}") String deadLetterTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.deadLetterTopic = deadLetterTopic;
    }

    public void publish(TransactionRequest tx) {
        try {
            String payload = objectMapper.writeValueAsString(tx);
            kafkaTemplate.send(topic, tx.getTransactionId(), payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize transaction", e);
        }
    }

    public void publishDeadLetter(TransactionRequest tx, String reason) {
        try {
            // Build a small envelope with the original transaction and reason
            com.fasterxml.jackson.databind.node.ObjectNode node = objectMapper.createObjectNode();
            node.set("transaction", objectMapper.valueToTree(tx));
            node.put("reason", reason == null ? "" : reason);
            String payload = objectMapper.writeValueAsString(node);
            kafkaTemplate.send(deadLetterTopic, tx.getTransactionId(), payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize dead-letter payload", e);
        }
    }
}

