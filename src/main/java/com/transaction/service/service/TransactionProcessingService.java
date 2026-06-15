package com.transaction.service.service;

import com.transaction.service.client.RiskClient;
import com.transaction.service.client.exception.RiskServiceUnavailableException;
import com.transaction.service.dto.TransactionRequest;
import com.transaction.service.kafka.KafkaProducerService;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.*;

@Service
public class TransactionProcessingService {

    private final RiskClient riskClient;
    private final java.util.Optional<KafkaProducerService> kafkaProducerService;
    private final Map<String, TransactionRecord> store = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public TransactionProcessingService(RiskClient riskClient, java.util.Optional<KafkaProducerService> kafkaProducerService) {
        this.riskClient = riskClient;
        this.kafkaProducerService = kafkaProducerService;
    }

    public void submit(TransactionRequest request) {
        TransactionRecord rec = new TransactionRecord(request);
        store.put(request.getTransactionId(), rec);
        executor.submit(() -> process(rec));
    }

    private void process(TransactionRecord rec) {
        TransactionRequest tx = rec.getRequest();
        try {
            rec.setStatus(TransactionRecord.Status.PENDING);
            var risk = riskClient.checkRisk(tx.getTransactionId());
            if (risk == null || risk.getResult() == null) {
                rec.setStatus(TransactionRecord.Status.FAILED);
                rec.setReason("Risk service returned invalid response");
                return;
            }

            if ("allowed".equalsIgnoreCase(risk.getResult())) {
                // publish if possible
                kafkaProducerService.ifPresent(p -> p.publish(tx));
                rec.setStatus(TransactionRecord.Status.ALLOWED);
            } else {
                rec.setStatus(TransactionRecord.Status.NOT_ALLOWED);
            }

        } catch (RiskServiceUnavailableException ex) {
            rec.setReason(ex.getMessage());
            if (kafkaProducerService.isPresent()) {
                // Still publish to dead-letter topic, but record status as FAILED (do not keep a separate DEAD_LETTERED status)
                kafkaProducerService.get().publishDeadLetter(tx, ex.getMessage());
            }
            rec.setStatus(TransactionRecord.Status.FAILED);
        } catch (Exception ex) {
            rec.setReason(ex.getMessage());
            rec.setStatus(TransactionRecord.Status.FAILED);
        }
    }

    public TransactionRecord getRecord(String transactionId) {
        return store.get(transactionId);
    }

    // simple POJO for storing status
    public static class TransactionRecord {
        public enum Status {PENDING, ALLOWED, NOT_ALLOWED, FAILED}

        private final TransactionRequest request;
        private volatile Status status = Status.PENDING;
        private volatile String reason;

        public TransactionRecord(TransactionRequest request) {
            this.request = request;
        }

        public TransactionRequest getRequest() {
            return request;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }
    }
}

