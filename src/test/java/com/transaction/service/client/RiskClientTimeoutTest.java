package com.transaction.service.client;

import com.transaction.service.client.exception.RiskServiceUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.net.SocketTimeoutException;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RiskClientTimeoutTest {

    @Test
    void checkRisk_whenRequestTimesOut_failsImmediatelyWithoutRetry() {
        TimeoutRestTemplate restTemplate = new TimeoutRestTemplate();
        RiskClient riskClient = new RiskClient(restTemplate, "http://localhost:1080/risk");

        RiskServiceUnavailableException ex = assertThrows(
                RiskServiceUnavailableException.class,
                () -> riskClient.checkRisk("tx-timeout")
        );

        assertEquals("Risk service request timed out after 10 seconds", ex.getMessage());
        assertEquals(1, restTemplate.getAttempts());
    }

    private static class TimeoutRestTemplate extends RestTemplate {
        private int attempts;

        int getAttempts() {
            return attempts;
        }

        @Override
        public <T> org.springframework.http.ResponseEntity<T> getForEntity(URI url, Class<T> responseType) {
            attempts++;
            throw new ResourceAccessException("Read timed out", new SocketTimeoutException("Read timed out"));
        }
    }
}

