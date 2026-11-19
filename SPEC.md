# Especificación: sistema de transferencias con evaluación de fraude asíncrona

> Documento de referencia para desarrollar el proyecto con Claude Code.
> La arquitectura ya está aprobada. Las decisiones marcadas como **[Decidido]** no se cambian sin preguntar.

## 1. Contexto y objetivo

- **Qué problema resuelve:** un sistema simplificado de transferencias entre cuentas con evaluación de fraude asíncrona. Al crear una transferencia, esta queda en estado `PENDING` y un segundo servicio evalúa su riesgo sin bloquear la API. Según el resultado, la transferencia se aprueba o se rechaza.
- **Propósito del proyecto:** portafolio y aprendizaje de arquitectura orientada a eventos con Kafka.

### Flujo de punta a punta

1. El cliente llama `POST /transfers` en transfer-service.
2. transfer-service valida los datos y, **en una misma transacción**, guarda la transferencia con estado `PENDING` y una fila en la tabla `outbox_events`.
3. Un publicador programado lee el outbox y publica el evento `transfer.created` en Kafka.
4. fraud-service consume `transfer.created`, aplica las reglas de riesgo, guarda el resultado y publica `transfer.evaluated` con la decisión (`APPROVED` o `REJECTED`) y el motivo.
5. transfer-service consume `transfer.evaluated` y actualiza la transferencia a `COMPLETED` o `REJECTED`.
6. El cliente consulta `GET /transfers/{id}` para ver el estado final.

### Reglas de fraude (configurables)

Se evalúan en este orden. Se guarda el **primer** motivo que aplique:

| Orden | Regla | Motivo |
|---|---|---|
| 1 | Cuenta origen igual a la destino | `SAME_ACCOUNT` |
| 2 | Monto mayor al umbral (por defecto 10.000.000 COP) | `AMOUNT_EXCEEDED` |
| 3 | Más de 3 transferencias del mismo origen en 60 segundos | `VELOCITY_EXCEEDED` |
| — | Ninguna aplica | `APPROVED`, motivo `null` |

Detalle de la regla de velocidad:

- Se rechaza la **cuarta** transferencia dentro de una ventana de 60 s.
- Cuentan **todos** los intentos evaluados del mismo `fromAccount`, incluidos los rechazados.
- La ventana se calcula con el `createdAt` del evento, no con la hora de procesamiento: se cuentan las evaluaciones con `transferCreatedAt` en `[createdAt - 60s, createdAt)`.
- El umbral de monto, el límite de transferencias y la ventana se configuran en `application.yml` (`fraud.rules.*`) y se pueden sobrescribir con variables de entorno.

## 2. Microservicios

### Servicio A: transfer-service (puerto 8080)

- **Responsabilidad:** recibir y gestionar el ciclo de vida de las transferencias.
- **Expone:**
  - `POST /transfers` — body: `{fromAccount, toAccount, amount, currency}` → `202 Accepted` con `{id, status: "PENDING"}`
  - `GET /transfers/{id}` → `200` con `{id, fromAccount, toAccount, amount, currency, status, reason, createdAt, updatedAt}`; `404` si no existe
  - `GET /transfers?fromAccount=...&page=0&size=20` → lista paginada; `fromAccount` es obligatorio
- **Validaciones de entrada:**
  - `fromAccount` y `toAccount`: obligatorios, no vacíos
  - `amount`: obligatorio, mayor que 0, máximo 2 decimales
  - `currency`: solo `COP` en esta versión **[Decidido]**; cualquier otra → `400`
  - Origen igual a destino **no** se valida en la API: responde `202` y fraud-service lo rechaza
- **Base de datos propia:** PostgreSQL (tablas `transfers` y `outbox_events`)
- **Produce a Kafka:** `transfer.created` (mediante outbox)
- **Consume de Kafka:** `transfer.evaluated` (consumer group: `transfer-service-group`)

### Servicio B: fraud-service (puerto 8081)

- **Responsabilidad:** evaluar el riesgo de cada transferencia aplicando las reglas de fraude.
- **Expone:** health checks (actuator) y `GET /evaluations/{transferId}` para consultar el resultado (`404` si no existe).
- **Base de datos propia:** PostgreSQL (tabla `evaluations`)
- **Produce a Kafka:** `transfer.evaluated`
- **Consume de Kafka:** `transfer.created` (consumer group: `fraud-service-group`)

### Modelo de datos

**transfer-service**

`transfers`

| Columna | Tipo | Notas |
|---|---|---|
| `id` | `UUID` | PK |
| `from_account` | `VARCHAR(34)` | Índice |
| `to_account` | `VARCHAR(34)` | |
| `amount` | `NUMERIC(19,2)` | |
| `currency` | `VARCHAR(3)` | |
| `status` | `VARCHAR(20)` | `PENDING`, `COMPLETED`, `REJECTED` |
| `reason` | `VARCHAR(50)` | Nulo si se aprueba |
| `created_at` | `TIMESTAMPTZ` | |
| `updated_at` | `TIMESTAMPTZ` | |

`outbox_events`

| Columna | Tipo | Notas |
|---|---|---|
| `id` | `UUID` | PK; se usa como `eventId` |
| `aggregate_id` | `UUID` | `transferId` |
| `topic` | `VARCHAR(100)` | |
| `message_key` | `VARCHAR(100)` | `fromAccount` |
| `payload` | `JSONB` | Evento serializado |
| `correlation_id` | `VARCHAR(100)` | Se copia al header de Kafka |
| `created_at` | `TIMESTAMPTZ` | |
| `published_at` | `TIMESTAMPTZ` | Nulo hasta publicarse; índice parcial sobre los no publicados |

**fraud-service**

`evaluations`

| Columna | Tipo | Notas |
|---|---|---|
| `id` | `UUID` | PK |
| `transfer_id` | `UUID` | **Único**: garantiza idempotencia |
| `from_account` | `VARCHAR(34)` | |
| `amount` | `NUMERIC(19,2)` | |
| `decision` | `VARCHAR(20)` | `APPROVED`, `REJECTED` |
| `reason` | `VARCHAR(50)` | |
| `transfer_created_at` | `TIMESTAMPTZ` | Para la regla de velocidad |
| `evaluated_at` | `TIMESTAMPTZ` | |

Índice compuesto `(from_account, transfer_created_at)` para la regla de velocidad.

## 3. Stack técnico

| Tema | Decisión |
|---|---|
| Lenguaje | Java 21 |
| Framework | Spring Boot **4.1.x** (la línea 4.0 pierde soporte el 31-dic-2026) con Spring for Apache Kafka |
| Build | Maven, con Maven Wrapper y un `pom.xml` agregador en la raíz |
| Base de datos | PostgreSQL, una base independiente por servicio |
| Persistencia | Spring Data JPA, IDs `UUID`, sin Lombok |
| Migraciones | **Flyway** **[Decidido]**, SQL plano en `db/migration` |
| Serialización Kafka | `JacksonJsonSerializer` / `JacksonJsonDeserializer` (Jackson 3; los antiguos `JsonSerializer`/`JsonDeserializer` están deprecados desde Spring Kafka 4.0) |
| Kafka en local | Imagen oficial `apache/kafka` en modo KRaft, un solo nodo, **versión fija** (no `latest`) |
| Kafka UI | `kafbat/kafka-ui`, opcional mediante un perfil de Docker Compose |
| Documentación API | springdoc-openapi **3.x** (`springdoc-openapi-starter-webmvc-ui`), la línea compatible con Spring Boot 4 |

Antes de escribir los `pom.xml` y el `docker-compose.yml`, verifica las versiones estables más recientes de Spring Boot 4.1.x, springdoc 3.x, `apache/kafka`, `kafbat/kafka-ui` y `postgres`, y fíjalas explícitamente.

## 4. Diseño de Kafka

### Topics

| Topic | Particiones | Réplicas | Clave | Productor | Consumer group |
|---|---|---|---|---|---|
| `transfer.created` | 3 | 1 | `fromAccount` **[Decidido]** | transfer-service | `fraud-service-group` |
| `transfer.evaluated` | 3 | 1 | `transferId` | fraud-service | `transfer-service-group` |
| `transfer.created.DLT` | 3 | 1 | La original | fraud-service | Ninguno |
| `transfer.evaluated.DLT` | 3 | 1 | La original | transfer-service | Ninguno |

- Réplicas = 1 porque en local hay un solo broker.
- **Por qué `fromAccount` como clave de `transfer.created`:** todas las transferencias de una misma cuenta van a la misma partición y se procesan en orden. Así la regla de velocidad no cuenta mal por procesamiento en paralelo. Cada transferencia individual sigue teniendo orden garantizado.
- Los topics se crean desde la aplicación con beans `NewTopic` (configuración en `config/`).

### Formato de los eventos (JSON)

`transfer.created`

```json
{
  "eventId": "uuid",
  "transferId": "uuid",
  "fromAccount": "string",
  "toAccount": "string",
  "amount": 150000.00,
  "currency": "COP",
  "createdAt": "2026-11-19T17:00:00Z"
}
```

`transfer.evaluated`

```json
{
  "eventId": "uuid",
  "transferId": "uuid",
  "decision": "APPROVED | REJECTED",
  "reason": "SAME_ACCOUNT | AMOUNT_EXCEEDED | VELOCITY_EXCEEDED | null",
  "evaluatedAt": "2026-11-19T17:00:01Z"
}
```

- Headers de Kafka: `correlationId` en todos los mensajes.
- Cada servicio define sus propios `record` para los eventos. **No** hay librería compartida: el contrato es el JSON. Para evitar acoplar por nombre de clase, desactiva los type headers y configura el tipo destino en el deserializador.

### Garantías de entrega

- Semántica **al menos una vez**: puede haber duplicados y ambos consumidores los toleran.
- Productores: `acks=all` y `enable.idempotence=true`.
- Consumidores: `enable.auto.commit=false`, commit por registro (`AckMode.RECORD`), `auto.offset.reset=earliest`.

### Outbox en transfer-service [Decidido]

- La transferencia y su fila en `outbox_events` se guardan en la **misma transacción**.
- Un publicador con `@Scheduled` (cada 1 s, configurable) lee lotes de eventos no publicados en orden de `created_at`, los envía a Kafka, espera la confirmación y marca `published_at`.
- Si el envío falla, el evento queda pendiente y se reintenta en la siguiente ejecución.
- Sin Debezium ni librerías adicionales.
- fraud-service **no** necesita outbox: si falla al publicar, el mensaje se reintenta y, gracias a la idempotencia, reenvía la decisión ya guardada.

### Idempotencia

- **fraud-service:** si recibe un `transferId` que ya existe en `evaluations`, no vuelve a evaluar; reenvía la decisión guardada. La restricción única en `transfer_id` protege contra condiciones de carrera.
- **transfer-service:** actualiza con `UPDATE ... WHERE id = ? AND status = 'PENDING'`. Si la transferencia ya no está en `PENDING`, el mensaje se ignora y se registra en el log.

### Manejo de errores

- `DefaultErrorHandler` con backoff exponencial: 3 reintentos (1 s, 2 s, 4 s) y luego `DeadLetterPublishingRecoverer` hacia el topic `.DLT` correspondiente.
- Los errores de deserialización van directo a la DLT, sin reintentos (`ErrorHandlingDeserializer`).
- **Limitación conocida:** si un evento llega a la DLT, la transferencia queda en `PENDING`. No hay proceso de reconciliación; se documenta en el README.

## 5. Infraestructura

- `docker-compose.yml` con: Kafka (KRaft), Kafka UI (perfil `ui`), dos PostgreSQL (una por servicio) y los 2 servicios.
- Health checks en Compose y `depends_on` con `condition: service_healthy`.
- Dockerfile multi-stage por servicio: build con Maven + JDK 21, ejecución con JRE 21 y usuario no root.
- Configuración por variables de entorno y perfiles de Spring: `local` (servicios corriendo en el IDE contra la infraestructura de Compose) y `docker` (todo en contenedores).
- `.env.example` con todas las variables.
- Levantar todo con un comando: `docker compose up --build` (y `docker compose --profile ui up --build` para incluir Kafka UI).

## 6. Calidad y operación

- Pruebas unitarias con JUnit 5 + Mockito: cada regla de fraude, el servicio de evaluación (incluida la idempotencia), el servicio de transferencias y el publicador del outbox.
- Pruebas de integración con Testcontainers (Kafka + PostgreSQL), con `@ServiceConnection`: flujo de punta a punta de cada servicio, duplicados y envío a DLT.
- Validación de entrada con Bean Validation, manejo global de excepciones con `@RestControllerAdvice` y respuestas `ProblemDetail` (RFC 9457).
- Códigos HTTP: `202` al crear, `200` al consultar, `400` en validación, `404` si no existe.
- Logging estructurado con el soporte nativo de Spring Boot (`logging.structured.format.console`), sin librerías extra.
- `correlationId`: se lee del header HTTP `X-Correlation-Id` o se genera si no viene; se guarda en el MDC, se copia al outbox, viaja en los headers de Kafka y se restaura en el MDC al consumir.
- Health checks con Actuator y documentación de la API con Swagger UI.

## 7. Estructura y entregables

### Arquitectura interna: por capas [Decidido]

Con una entidad principal por servicio, la arquitectura hexagonal duplicaría clases sin aportar mucho. Cada servicio usa los paquetes `api`, `service`, `domain`, `repository`, `messaging` y `config`. Las reglas de fraude se implementan como una clase por regla detrás de la interfaz `FraudRule`, para probarlas por separado y agregar reglas sin tocar las demás.

### Estructura de carpetas (monorepo)

```text
transfer-fraud-system/
├── pom.xml                         # Agregador Maven
├── mvnw / mvnw.cmd / .mvn/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
├── SPEC.md                         # Este documento
├── transfer-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── java/com/example/transfer/
│       │   │   ├── TransferServiceApplication.java
│       │   │   ├── api/            # Controller, DTOs, manejo global de errores
│       │   │   ├── service/        # Casos de uso
│       │   │   ├── domain/         # Transfer, TransferStatus
│       │   │   ├── repository/     # Transfers y outbox
│       │   │   ├── messaging/      # Publicador del outbox, consumidor y eventos
│       │   │   └── config/         # Kafka, topics, OpenAPI, filtro de correlationId
│       │   └── resources/
│       │       ├── application.yml
│       │       ├── application-local.yml
│       │       ├── application-docker.yml
│       │       └── db/migration/
│       └── test/java/com/example/transfer/
│           ├── unit/
│           └── integration/        # Testcontainers: Kafka + PostgreSQL
└── fraud-service/
    ├── Dockerfile
    ├── pom.xml
    └── src/
        ├── main/
        │   ├── java/com/example/fraud/
        │   │   ├── FraudServiceApplication.java
        │   │   ├── api/            # GET /evaluations/{transferId}
        │   │   ├── service/        # Evaluación e idempotencia
        │   │   ├── rules/          # FraudRule y una clase por regla
        │   │   ├── domain/         # Evaluation, Decision
        │   │   ├── repository/
        │   │   ├── messaging/      # Consumidor, productor y eventos
        │   │   └── config/
        │   └── resources/          # Misma estructura que transfer-service
        └── test/java/com/example/fraud/
            ├── unit/
            └── integration/
```

### README

- Arquitectura con diagrama en Mermaid.
- Cómo levantar todo con un comando.
- Ejemplos de `curl` para cada endpoint y para probar cada regla de fraude.
- Cómo ver los mensajes en Kafka UI.
- Decisiones de diseño y limitaciones conocidas.

### Convenciones

- `.gitignore` para Java, Maven, IDEs y `.env`.
- Commits con Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `build:`), con el servicio como alcance cuando aplique: `feat(fraud-service): add velocity rule`.

## 8. Forma de trabajo en Claude Code

La arquitectura ya está aprobada. Trabaja por fases y **detente al final de cada una** para que yo revise antes de seguir:

| Fase | Contenido | Verificación antes de terminar |
|---|---|---|
| **(a) Infraestructura y esqueleto** | `pom.xml` raíz y de cada servicio, Maven Wrapper, `docker-compose.yml`, Dockerfiles, `.env.example`, `.gitignore`, aplicaciones Spring vacías con Actuator y perfiles | `./mvnw -q verify` compila y `docker compose up --build` levanta todo con los health checks en verde |
| **(b) transfer-service** | Endpoints, validaciones, migraciones Flyway, outbox, publicador, consumidor de `transfer.evaluated`, correlationId, OpenAPI | `./mvnw -pl transfer-service verify` pasa |
| **(c) fraud-service** | Reglas, evaluación, idempotencia, consumidor, productor, DLT, endpoint de consulta | `./mvnw -pl fraud-service verify` pasa |
| **(d) Pruebas y README** | Completar pruebas unitarias e integración, README con Mermaid y `curl` | `./mvnw verify` pasa y el flujo de punta a punta funciona con `docker compose` |

Reglas:

1. Escribe archivos completos, sin fragmentos `...` ni pseudocódigo.
2. Ejecuta los comandos de verificación de cada fase y corrige los errores antes de darla por terminada. No digas que algo funciona sin haberlo ejecutado.
3. Haz un commit por fase (o por bloque lógico dentro de la fase) con Conventional Commits.
4. Si algo no está definido en este documento, pregúntame antes de asumir. Si asumes algo, dilo explícitamente al final de la fase.
5. Al terminar cada fase, resume en pocas líneas qué se hizo, qué decisiones tomaste y cualquier supuesto nuevo.

## 9. Restricciones

- No agregues librerías ni servicios que no estén en este documento (sin API Gateway, service discovery, Kubernetes, Lombok ni Debezium).
- Prefiere la solución simple y legible sobre la sobreingeniería.
- Explica brevemente las decisiones de diseño importantes en el código solo cuando no sean obvias, y en el README.