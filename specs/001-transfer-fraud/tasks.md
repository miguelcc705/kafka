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

## Fase (b): transfer-service ✅

**Verificación:** `./mvnw -pl transfer-service verify` pasa (16 pruebas: 9 unitarias y 7 de integración).

- [x] Dependencias de prueba (Testcontainers Kafka + PostgreSQL, `spring-boot-starter-webmvc-test`)
- [x] Migración `V1__create_transfers_and_outbox.sql` (tablas, índice en `from_account`, índice parcial del outbox)
- [x] `domain`: `Transfer`, `TransferStatus`, `OutboxEvent`
- [x] `repository`: `TransferRepository` (con `UPDATE ... WHERE status = 'PENDING'`), `OutboxEventRepository`
- [x] `api`: `CreateTransferRequest` con Bean Validation, DTOs de respuesta y `TransferController` (`POST`, `GET /{id}`, `GET ?fromAccount`)
- [x] `api`: `GlobalExceptionHandler` con `ProblemDetail` (400 con detalle por campo, y 404)
- [x] `config`: `CorrelationIdFilter` (header `X-Correlation-Id` ↔ MDC)
- [x] `service`: `TransferService.create` guarda la transferencia y el outbox en la misma transacción
- [x] `messaging`: `TransferCreatedEvent`, `TransferEvaluatedEvent`
- [x] `messaging`: `OutboxPublisher` con `@Scheduled` y `transfer.outbox.*` configurable
- [x] `config`: productor (`acks=all`, idempotencia, `JacksonJsonSerializer` sin type headers) y header `correlationId`
- [x] `config`: beans `NewTopic` (`KafkaTopics`)
- [x] `messaging`: `TransferEvaluatedListener` con `ErrorHandlingDeserializer`, `DefaultErrorHandler` y DLT `.DLT`
- [x] `service`: aplicar la decisión de forma idempotente (log si no está en `PENDING`)
- [x] `config`: OpenAPI y Swagger UI (`/swagger-ui.html`)
- [x] Pruebas unitarias de `TransferService` y `OutboxPublisher`
- [x] Integración adelantada de la fase (d): outbox → Kafka con correlationId, decisión y duplicado, DLT por JSON inválido (sin reintentos) y por fallo de procesamiento (tras 1 s + 2 s + 4 s), validaciones y paginación
- [x] Prueba manual con `docker compose`: `POST` → `transfer.created` con clave, header y monto `150000.00`
- [x] Verificación y commit `feat(transfer-service): ...`

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

- [x] Pruebas de integración de transfer-service: flujo, duplicados, DLT (hechas en la fase b)
- [ ] Pruebas de integración de fraud-service: flujo, duplicados, DLT
- [ ] README: diagrama Mermaid, cómo levantar todo, `curl` por endpoint y por regla, Kafka UI, decisiones y limitaciones
- [ ] Prueba manual de punta a punta con `docker compose up --build`
- [ ] Verificación y commit(s) `test: ...` / `docs: ...`
