# Spring Kafka Reliable Processing

Java 17 / Spring Boot 3.3 example of at-least-once Kafka consumption with database idempotency.

## Implemented

- Manual Kafka acknowledgment after a transactional service call returns.
- PostgreSQL unique event claim via INSERT ... ON CONFLICT DO NOTHING.
- Business event payload inserted in the same database transaction as the claim.
- Duplicate deliveries do not overwrite business data.
- Configured exponential backoff, recovery and dead-letter publishing to events.DLT.
- Testcontainers PostgreSQL integration test for duplicate delivery and business persistence.
- Unit tests for listener acknowledgment, processor and error handler.

## Request flow

Kafka event -> listener -> PostgreSQL transaction -> claim unique event ID -> write business event (if new) -> commit -> acknowledge Kafka offset.

On failure the listener throws; Spring Kafka's error handler retries and then publishes to a dead-letter topic. The database and Kafka offset commits are not atomic; a crash between commits can cause redelivery, which the database uniqueness constraint handles.

## Run

Java 17, Maven, PostgreSQL, Kafka and Docker (for integration tests) are required. Set DB_URL, DB_USER, DB_PASSWORD and KAFKA_BOOTSTRAP_SERVERS as appropriate. Provision events and events.DLT with matching partition counts.

    mvn clean verify
    mvn spring-boot:run

## Still to verify

Kafka broker end-to-end retry and DLT behavior have not yet been tested. Production deployments also need monitoring, dead-letter replay procedures, schema validation and domain-specific business logic. IdempotencyGate is an in-memory teaching example, not the durable production gate.
