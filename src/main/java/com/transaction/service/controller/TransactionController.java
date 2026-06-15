package com.transaction.service.controller;

import com.transaction.service.client.RiskClient;
import com.transaction.service.dto.TransactionRequest;
import com.transaction.service.dto.TransactionStatusResponse;
import com.transaction.service.kafka.KafkaProducerService;
import com.transaction.service.service.TransactionProcessingService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/transactions")
public class TransactionController {

    private final RiskClient riskClient;
    private final java.util.Optional<KafkaProducerService> kafkaProducerService;
    private final TransactionProcessingService processingService;

    public TransactionController(RiskClient riskClient, java.util.Optional<KafkaProducerService> kafkaProducerService, TransactionProcessingService processingService) {
        this.riskClient = riskClient;
        this.kafkaProducerService = kafkaProducerService;
        this.processingService = processingService;
    }

    @PostMapping("/p2p")
    @ResponseBody
    public ResponseEntity<String> createP2P(@RequestBody TransactionRequest request) {
        // Accept asynchronously: store and process in background
        if (request.getTransactionId() == null || request.getTransactionId().isEmpty()) {
            return ResponseEntity.badRequest().body("transactionId is required");
        }
        processingService.submit(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("Transaction received and processing");
    }

    @GetMapping("/{transactionId}/status")
    @ResponseBody
    public ResponseEntity<TransactionStatusResponse> getStatus(@org.springframework.web.bind.annotation.PathVariable String transactionId) {
        var rec = processingService.getRecord(transactionId);
        if (rec == null) {
            return ResponseEntity.notFound().build();
        }
        TransactionStatusResponse resp = new TransactionStatusResponse(transactionId, rec.getStatus().name(), rec.getReason());
        return ResponseEntity.ok(resp);
    }
}

