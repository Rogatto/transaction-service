package com.search.service.client;

import com.search.service.dto.RiskResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpStatusCodeException;

import com.search.service.client.exception.RiskServiceUnavailableException;

@Component
public class RiskClient {

    private final RestTemplate restTemplate;
    private final String riskApiUrl;

    public RiskClient(RestTemplate restTemplate, @Value("${risk.api.url}") String riskApiUrl) {
        this.restTemplate = restTemplate;
        this.riskApiUrl = riskApiUrl;
    }

    public RiskResponse checkRisk(String transactionId) {
        // The risk API is a simple GET that returns {"result":"allowed"} or {"result":"notAllowed"}
        // It accepts a query parameter transactionId
        URI uri = UriComponentsBuilder.fromUriString(riskApiUrl)
                .queryParam("transactionId", transactionId)
                .build()
                .toUri();

        final int maxAttempts = 3;
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                ResponseEntity<RiskResponse> resp = restTemplate.getForEntity(uri, RiskResponse.class);
                int statusCode = resp.getStatusCode().value();
                if (statusCode >= 200 && statusCode < 300) {
                    return resp.getBody();
                }

                // For 5xx server errors, retry up to maxAttempts
                if (statusCode >= 500 && statusCode < 600) {
                    if (attempt >= maxAttempts) {
                        throw new RiskServiceUnavailableException("Risk service returned server error after " + attempt + " attempts");
                    }
                    // brief pause before retrying
                    try { Thread.sleep(500L); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    continue;
                }

                // For other statuses (4xx), treat as non-retryable and return body (may be null)
                return resp.getBody();

            } catch (HttpStatusCodeException ex) {
                int statusCode = ex.getStatusCode() != null ? ex.getStatusCode().value() : -1;
                if (statusCode >= 500 && statusCode < 600) {
                    if (attempt >= maxAttempts) {
                        throw new RiskServiceUnavailableException("Risk service returned server error after " + attempt + " attempts", ex);
                    }
                    try { Thread.sleep(500L); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    continue;
                }
                // Non-5xx errors - rethrow as runtime
                throw ex;
            } catch (RiskServiceUnavailableException e) {
                throw e;
            } catch (Exception e) {
                // For other exceptions (e.g., IO), consider them retryable up to maxAttempts
                if (attempt >= maxAttempts) {
                    throw new RiskServiceUnavailableException("Risk service unavailable after " + attempt + " attempts", e);
                }
                try { Thread.sleep(500L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }
}

