# Plan técnico: transferencias con evaluación de fraude asíncrona

- **Especificación:** [SPEC.md](../../SPEC.md)
- **Principios:** [constitution.md](../../docs/constitution.md)
- **Tareas:** [tasks.md](tasks.md)

## 1. Resumen

Hay dos microservicios Spring Boot que se comunican solo por Kafka:

- **transfer-service** recibe las transferencias por HTTP, las guarda como `PENDING` y publica `transfer.created` mediante un outbox.
- **fraud-service** evalúa cada transferencia con tres reglas y publica `transfer.evaluated`.

Con esa decisión, transfer-service actualiza el estado final de la transferencia.

## 2. Versiones fijadas

Se verificaron en Maven Central y Docker Hub el 2026-11-19.

| Componente | Versión |
|---|---|
| Java | 21 (imágenes `eclipse-temurin:21.0.13_9-jdk-noble` / `-jre-noble`) |
| Spring Boot | 4.1.2 |
| Spring for Apache Kafka | 4.1.2 (gestionada por Boot) |
| Cliente Kafka | 4.2.2 (gestionado por Boot) |
| Flyway | 12.4.0 (gestionado por Boot) |
| Driver PostgreSQL | 42.7.14 (gestionado por Boot) |
| Testcontainers | 2.0.5 (gestionado por Boot) |
| springdoc-openapi | 3.2.0 |
| Maven / Maven Wrapper | 3.9.16 / 3.3.4 |
| Broker Kafka | `apache/kafka:4.4.0` (KRaft, un nodo) |
| Kafka UI | `kafbat/kafka-ui:v1.5.0` |
| PostgreSQL | `postgres:18.7-alpine` |

## 3. Arquitectura

### Componentes y puertos

| Componente | Puerto en el host | Puerto interno | Notas |
|---|---|---|---|
| transfer-service | 8080 | 8080 | |
| fraud-service | 8081 | 8081 | |
| Kafka | 9092 | 29092 (contenedores), 9093 (controller) | Dos listeners: `kafka:29092` para los contenedores y `localhost:9092` para el IDE |
| transfer-db | 5432 | 5432 | Base `transfers` |
| fraud-db | 5433 | 5432 | Base `fraud` |
| Kafka UI | 8090 | 8080 | Solo con `--profile ui` |

Todos los puertos del host se pueden cambiar desde `.env`.

### Perfiles de Spring

- `local` (el que se usa por defecto): el servicio corre en el IDE contra la infraestructura de Compose a través de `localhost`.
- `docker`: todo corre en contenedores. Usa los nombres de servicio como hosts y activa los logs estructurados (ECS).

### Paquetes por servicio

| Paquete | transfer-service | fraud-service |
|---|---|---|
| `api` | `TransferController`, DTOs, `GlobalExceptionHandler` | `EvaluationController`, `GlobalExceptionHandler` |
| `service` | `TransferService` (crear, consultar, aplicar decisión) | `EvaluationService` (evaluar con idempotencia) |
| `domain` | `Transfer`, `TransferStatus`, `OutboxEvent` | `Evaluation`, `Decision` |
| `rules` | — | `FraudRule`, `SameAccountRule`, `AmountExceededRule`, `VelocityExceededRule`, `FraudRulesProperties` |
| `repository` | `TransferRepository`, `OutboxEventRepository` | `EvaluationRepository` |
| `messaging` | `OutboxPublisher`, `TransferEvaluatedListener`, eventos | `TransferCreatedListener`, `EvaluationPublisher`, eventos |
| `config` | Kafka, topics, OpenAPI, `CorrelationIdFilter` | Kafka, topics, OpenAPI, `CorrelationIdFilter` |

## 4. Decisiones de diseño por tema

### Persistencia

- Esquema creado con Flyway (`V1__...sql`) y `ddl-auto: validate`, para que Hibernate solo verifique que el esquema coincida con las entidades.
- IDs `UUID` generados en la aplicación.
- Índice parcial sobre `outbox_events (created_at) WHERE published_at IS NULL`.
- Índice compuesto `evaluations (from_account, transfer_created_at)` y restricción única en `evaluations.transfer_id`.

### Outbox (transfer-service)

1. `TransferService.create` guarda en una sola `@Transactional` la transferencia `PENDING` y la fila del outbox, con el payload JSON y el `correlationId`.
2. `OutboxPublisher` corre con `@Scheduled(fixedDelayString = "${transfer.outbox.poll-interval:1s}")`. Lee un lote ordenado por `created_at`, envía cada evento con `KafkaTemplate.send(...).get()` y marca `published_at`.
3. Si un envío falla, se detiene el lote. El evento sigue pendiente y se reintenta en la siguiente ejecución, lo que conserva el orden.

### Kafka

- Serialización con `JacksonJsonSerializer` / `JacksonJsonDeserializer`, sin type headers y con el tipo destino configurado en el deserializador.
- `ErrorHandlingDeserializer` envuelve al deserializador JSON.
- `DefaultErrorHandler` con `ExponentialBackOff` (1 s inicial, multiplicador 2, 3 reintentos) y `DeadLetterPublishingRecoverer` hacia `<topic>.DLT`.
- Topics creados con beans `NewTopic` (3 particiones, 1 réplica). **Supuesto:** cada servicio declara todos los topics que produce o consume. Como `NewTopic` no falla si el topic ya existe, el orden de arranque deja de importar.

### Idempotencia

- **fraud-service:** si el `transferId` ya existe, reenvía la decisión guardada. Si dos mensajes compiten, la violación de la restricción única se resuelve leyendo la evaluación existente.
- **transfer-service:** `UPDATE transfers SET status=?, reason=?, updated_at=? WHERE id=? AND status='PENDING'`. Si actualiza 0 filas, registra el mensaje en el log y lo ignora.

### Regla de velocidad

Cuenta las evaluaciones del mismo `fromAccount` con `transfer_created_at` en `[createdAt - ventana, createdAt)`. Rechaza la transferencia si el conteo es mayor o igual que `velocity-max-transfers` (3), es decir, rechaza la cuarta.

### API y errores

- Bean Validation en el DTO de entrada (`@NotBlank`, `@Positive`, `@Digits(fraction = 2)`, y `currency` restringida a `COP`).
- `@RestControllerAdvice` que devuelve `ProblemDetail` con 400 (validación) y 404 (no existe).
- `CorrelationIdFilter` lee `X-Correlation-Id` o genera uno nuevo, lo pone en el MDC y lo devuelve en la respuesta.

## 5. Estrategia de pruebas

- **Unitarias (JUnit 5 + Mockito):** cada regla de fraude, `EvaluationService` (incluida la idempotencia), `TransferService` y `OutboxPublisher`.
- **Integración (Testcontainers con `@ServiceConnection`):** Kafka + PostgreSQL por servicio. Cubren el flujo completo de cada servicio, los duplicados y el envío a la DLT.

## 6. Riesgos y limitaciones conocidas

- Si un evento llega a la DLT, la transferencia queda en `PENDING` porque no hay reconciliación. Se documentará en el README.
- En la máquina de desarrollo el puerto 5432 ya está ocupado por un PostgreSQL local. Se usa `TRANSFER_DB_HOST_PORT=5434` en `.env`.
- No hay JDK 21 instalado en el host. Se compila con el JDK 24 y `--release 21`, y las imágenes Docker usan Temurin 21.

## 7. Supuestos registrados

| Fase | Supuesto |
|---|---|
| a | La raíz del monorepo es la carpeta actual, no una subcarpeta `transfer-fraud-system/`. |
| a | Los logs estructurados (ECS) solo están activos en el perfil `docker`. |
| a | `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`: solo existen los topics declarados con `NewTopic`. |
| a | El health check de Compose usa `/actuator/health/readiness`. |
| b/c | Cada servicio declara con `NewTopic` todos los topics que usa (pendiente de confirmar). |
| b | Se mantiene Spring MVC y no WebFlux: JPA, JDBC y el listener de Kafka son bloqueantes, y el SPEC fija `springdoc-openapi-starter-webmvc-ui`. |
| b | `fromAccount` y `toAccount` admiten hasta 34 caracteres (el tamaño de la columna); `amount` admite hasta 17 enteros y 2 decimales (`NUMERIC(19,2)`). |
| b | `GET /transfers` ordena de la más reciente a la más antigua y limita `size` a 1–100. |
| b | Un `X-Correlation-Id` vacío o de más de 100 caracteres (el tamaño de la columna) se reemplaza por un UUID nuevo. El valor efectivo se devuelve en la respuesta. |
| b | Si un evento no puede ir a la DLT (por ejemplo, Kafka caído), se reintenta indefinidamente: se bloquea esa partición, pero no se pierde el mensaje. |

## 8. Hallazgos de la implementación

- **Sufijo de la DLT:** en Spring Kafka 4 el `DeadLetterPublishingRecoverer` usa `-dlt` por defecto. Se configura un destino explícito: `<topic>.DLT` en la misma partición.
- **Precisión del monto en el outbox:** releer el payload como árbol JSON convertía `150000.00` en el double `150000.0`. El publicador envía el JSON guardado tal cual (`RawValue`).
- **Productor de la DLT:** necesita un serializador para `byte[]` (cuando falla la deserialización se reenvían los bytes originales) y otro JSON para el evento. Se usa `DelegatingByTypeSerializer` sobre una copia de la configuración del productor de Boot.
- **Testcontainers:** los contenedores se inician una vez por JVM en un bloque estático y no con `@Container`. Así no se detienen antes de que Spring cierre el contexto cacheado.
