# Transaction Service

A Spring Boot application that processes P2P transactions asynchronously. It integrates with a **Risk API** to evaluate each transaction and publishes approved/rejected events to **Apache Kafka**.

## Tech Stack

- Java 21
- Spring Boot 4.0.6
- Apache Kafka
- MockServer (for local Risk API simulation)

## How It Works

1. A transaction is submitted via `POST /transactions/p2p`.
2. The service calls the Risk API to check if the transaction is allowed.
3. The result is published to a Kafka topic (`transactions`).
4. If the Risk API is unavailable, the transaction is sent to a dead-letter topic (`transaction-dead-letter`).
5. The status of a transaction can be queried via `GET /transactions/{transactionId}/status`.

## API Endpoints

| Method | Endpoint                              | Description                     |
|--------|---------------------------------------|---------------------------------|
| POST   | `/transactions/p2p`                   | Submit a new P2P transaction    |
| GET    | `/transactions/{transactionId}/status`| Get the status of a transaction |

### Request Body (POST /transactions/p2p)

```json
{
  "transactionId": "abc123",
  "amount": 100.0,
  "sender": "user1",
  "receiver": "user2"
}
```

### Status Response

```json
{
  "transactionId": "abc123",
  "status": "APPROVED",
  "reason": null
}
```

## Running Locally

### Prerequisites

- Java 21
- Docker & Docker Compose

### 1. Start infrastructure with Docker Compose

```bash
docker-compose up -d
```

This starts:

| Service      | Description                        | Port  |
|--------------|------------------------------------|-------|
| `mockServer` | Simulates the Risk API             | 1080  |
| `zookeeper`  | Kafka coordination service         | 2181  |
| `kafka`      | Kafka broker                       | 9092  |

### 2. Build and run the application

```bash
./gradlew bootRun
```

The application will be available at `http://localhost:8080`.

### 3. Stop infrastructure

```bash
docker-compose down
```

## Configuration

Key properties in `src/main/resources/application.properties`:

| Property                       | Default                        | Description                        |
|--------------------------------|--------------------------------|------------------------------------|
| `risk.api.url`                 | `http://localhost:1080/risk`   | Risk API endpoint                  |
| `kafka.bootstrap-servers`      | `localhost:9092`               | Kafka broker address               |
| `kafka.topic.transactions`     | `transactions`                 | Main Kafka topic                   |
| `kafka.topic.dead-letter`      | `transaction-dead-letter`      | Dead-letter Kafka topic            |
| `kafka.enabled`                | `true`                         | Enable/disable Kafka               |

## Running Tests

```bash
./gradlew test
```

