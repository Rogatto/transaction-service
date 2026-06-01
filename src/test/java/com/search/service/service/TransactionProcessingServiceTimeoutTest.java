package com.search.service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.search.service.client.RiskClient;
import com.search.service.client.exception.RiskServiceUnavailableException;
import com.search.service.dto.RiskResponse;
import com.search.service.dto.TransactionRequest;
import com.search.service.kafka.KafkaProducerService;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransactionProcessingServiceTimeoutTest {

	@Test
	void submit_whenRiskTimeouts_marksFailedAndPublishesDeadLetterWithTimeoutReason() throws InterruptedException {
		String timeoutMessage = "Risk service request timed out after 10 seconds";
		TimeoutRiskClient riskClient = new TimeoutRiskClient(timeoutMessage);
		CapturingKafkaProducerService kafkaProducerService = new CapturingKafkaProducerService();
		TransactionProcessingService service = new TransactionProcessingService(riskClient, Optional.of(kafkaProducerService));

		TransactionRequest request = new TransactionRequest();
		request.setTransactionId("tx-timeout-dlq");
		request.setFromAccount("A");
		request.setToAccount("B");
		request.setAmount(BigDecimal.TEN);
		request.setCurrency("USD");

		service.submit(request);

		TransactionProcessingService.TransactionRecord record = null;
		for (int i = 0; i < 20; i++) {
			Thread.sleep(100);
			record = service.getRecord("tx-timeout-dlq");
			if (record != null && record.getStatus() == TransactionProcessingService.TransactionRecord.Status.FAILED) {
				break;
			}
		}

		assertNotNull(record);
		assertEquals(TransactionProcessingService.TransactionRecord.Status.FAILED, record.getStatus());
		assertEquals(timeoutMessage, record.getReason());
		assertEquals("tx-timeout-dlq", kafkaProducerService.lastDeadLetterTransactionId);
		assertEquals(timeoutMessage, kafkaProducerService.lastDeadLetterReason);
	}

	private static class TimeoutRiskClient extends RiskClient {
		private final String timeoutMessage;

		TimeoutRiskClient(String timeoutMessage) {
			super(new RestTemplate(), "http://localhost:1080/risk");
			this.timeoutMessage = timeoutMessage;
		}

		@Override
		public RiskResponse checkRisk(String transactionId) {
			throw new RiskServiceUnavailableException(timeoutMessage);
		}
	}

	private static class CapturingKafkaProducerService extends KafkaProducerService {
		private String lastDeadLetterTransactionId;
		private String lastDeadLetterReason;

		CapturingKafkaProducerService() {
			super((KafkaTemplate<String, String>) null, new ObjectMapper(), "transactions", "transaction-dead-letter");
		}

		@Override
		public void publishDeadLetter(TransactionRequest tx, String reason) {
			this.lastDeadLetterTransactionId = tx.getTransactionId();
			this.lastDeadLetterReason = reason;
		}
	}
}

