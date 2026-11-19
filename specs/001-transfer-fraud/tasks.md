# Tareas por fase

- **Plan:** [plan.md](plan.md)
- **Especificación:** [SPEC.md](../../SPEC.md)

Cada fase termina con su comando de verificación en verde, un commit y una pausa para la revisión.

## Fase (a): Infraestructura y esqueleto ✅

**Verificación:** `./mvnw -q verify` compila y `docker compose up --build` levanta todo con los health checks en verde.

- [x] Verificar y fijar las versiones (Boot, springdoc, Kafka, Kafka UI, PostgreSQL, Temurin)
- [x] `pom.xml` agregador en la raíz con `spring-boot-starter-parent` 4.1.2
- [x] `pom.xml` de transfer-service y fraud-service con sus dependencias
- [x] Maven Wrapper (`mvnw`, `mvnw.cmd`, `.mvn/`)
- [x] `TransferServiceApplication` y `FraudServiceApplication` vacías
- [x] `application.yml`, `application-local.yml` y `application-docker.yml` en cada servicio
- [x] Propiedades `fraud.rules.*` sobrescribibles con variables de entorno
- [x] Dockerfile multi-stage por servicio (JDK 21 → JRE 21, usuario no root)
- [x] `docker-compose.yml`: Kafka KRaft, dos PostgreSQL, los dos servicios y Kafka UI (perfil `ui`)
- [x] Health checks en Compose y `depends_on` con `service_healthy`
- [x] `.env.example`, `.gitignore`, `.dockerignore`
- [x] Verificación ejecutada y commit `build: add project skeleton, docker compose and maven wrapper`

## Fase (b): transfer-service

**Verificación:** `./mvnw -pl transfer-service verify` pasa.

- [ ] Dependencias de prueba (Testcontainers Kafka + PostgreSQL)
- [ ] Migración `V1__create_transfers_and_outbox.sql` (tablas, índice en `from_account`, índice parcial del outbox)
- [ ] `domain`: `Transfer`, `TransferStatus`, `OutboxEvent`
- [ ] `repository`: `TransferRepository` (con `UPDATE ... WHERE status = 'PENDING'`), `OutboxEventRepository`
- [ ] `api`: `CreateTransferRequest` con Bean Validation, DTOs de respuesta y `TransferController` (`POST`, `GET /{id}`, `GET ?fromAccount`)
- [ ] `api`: `GlobalExceptionHandler` con `ProblemDetail` (400 y 404)
- [ ] `config`: `CorrelationIdFilter` (header `X-Correlation-Id` ↔ MDC)
- [ ] `service`: `TransferService.create` guarda la transferencia y el outbox en la misma transacción
- [ ] `messaging`: `TransferCreatedEvent`, `TransferEvaluatedEvent`
- [ ] `messaging`: `OutboxPublisher` con `@Scheduled` y el intervalo configurable
- [ ] `config`: productor (`acks=all`, idempotencia, `JacksonJsonSerializer` sin type headers) y header `correlationId`
- [ ] `config`: beans `NewTopic`
- [ ] `messaging`: `TransferEvaluatedListener` con `ErrorHandlingDeserializer`, `DefaultErrorHandler` y DLT
- [ ] `service`: aplicar la decisión de forma idempotente (log si no está en `PENDING`)
- [ ] `config`: OpenAPI y Swagger UI
- [ ] Pruebas unitarias de `TransferService` y `OutboxPublisher`
- [ ] Verificación y commit(s) `feat(transfer-service): ...`

## Fase (c): fraud-service

**Verificación:** `./mvnw -pl fraud-service verify` pasa.

- [ ] Migración `V1__create_evaluations.sql` (restricción única en `transfer_id`, índice compuesto)
- [ ] `domain`: `Evaluation`, `Decision`
- [ ] `repository`: `EvaluationRepository` (incluido el conteo de la ventana de velocidad)
- [ ] `rules`: `FraudRule`, `FraudRulesProperties` y las reglas `SameAccountRule`, `AmountExceededRule`, `VelocityExceededRule` (en ese orden)
- [ ] `service`: `EvaluationService` aplica la primera regla que coincida y es idempotente por `transferId`
- [ ] `messaging`: eventos, `TransferCreatedListener` y publicación de `transfer.evaluated`
- [ ] `config`: consumidor, productor, `DefaultErrorHandler` + DLT, `NewTopic`, correlationId
- [ ] `api`: `GET /evaluations/{transferId}` (404 si no existe) y `ProblemDetail`
- [ ] `config`: OpenAPI y Swagger UI
- [ ] Pruebas unitarias de cada regla y de `EvaluationService`
- [ ] Verificación y commit(s) `feat(fraud-service): ...`

## Fase (d): Pruebas y README

**Verificación:** `./mvnw verify` pasa y el flujo de punta a punta funciona con `docker compose`.

- [ ] Pruebas de integración de transfer-service: flujo, duplicados, DLT
- [ ] Pruebas de integración de fraud-service: flujo, duplicados, DLT
- [ ] README: diagrama Mermaid, cómo levantar todo, `curl` por endpoint y por regla, Kafka UI, decisiones y limitaciones
- [ ] Prueba manual de punta a punta con `docker compose up --build`
- [ ] Verificación y commit(s) `test: ...` / `docs: ...`
