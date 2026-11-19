package com.example.fraud.integration;

import com.example.fraud.config.CorrelationId;
import com.example.fraud.config.KafkaTopics;
import com.example.fraud.repository.EvaluationRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class FraudServiceIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.7-alpine");

    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

    // Se inician una vez por JVM y Testcontainers (Ryuk) los elimina al terminar. Con @Container se detendrían al
    // acabar la clase, antes de que Spring cierre el contexto cacheado, y el cierre quedaría esperando a la base.
    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private EvaluationRepository evaluationRepository;

    @Test
    void approvedTransferPublishesDecisionWithCorrelationIdAndIsQueryable() throws Exception {
        String fromAccount = account();
        String transferId = UUID.randomUUID().toString();
        sendRaw(KafkaTopics.TRANSFER_CREATED, fromAccount,
                createdJson(transferId, fromAccount, "ACC-DEST", "150000.00", Instant.now()), "corr-it-1");

        ConsumerRecord<String, String> record = consumeOne(KafkaTopics.TRANSFER_EVALUATED, transferId);

        JsonNode event = jsonMapper.readTree(record.value());
        assertThat(event.get("transferId").asString()).isEqualTo(transferId);
        assertThat(event.get("decision").asString()).isEqualTo("APPROVED");
        assertThat(event.get("reason").isNull()).isTrue();
        assertThat(event.get("eventId").asString()).isNotBlank();
        assertThat(Instant.parse(event.get("evaluatedAt").asString())).isNotNull();
        assertThat(headerValue(record, CorrelationId.KAFKA_HEADER)).isEqualTo("corr-it-1");
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();

        mockMvc.perform(get("/evaluations/{transferId}", transferId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transferId").value(transferId))
                .andExpect(jsonPath("$.fromAccount").value(fromAccount))
                .andExpect(jsonPath("$.decision").value("APPROVED"))
                .andExpect(jsonPath("$.reason").doesNotExist())
                .andExpect(jsonPath("$.id").value(event.get("eventId").asString()))
                // El monto conserva sus 2 decimales.
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"amount\":150000.00")));
    }

    @Test
    void rulesAreAppliedInOrderAndTheFirstReasonIsStored() throws Exception {
        String sameAccount = account();
        // Aplican SAME_ACCOUNT y AMOUNT_EXCEEDED: se guarda la primera.
        String sameAccountId = UUID.randomUUID().toString();
        sendRaw(KafkaTopics.TRANSFER_CREATED, sameAccount,
                createdJson(sameAccountId, sameAccount, sameAccount, "20000000.00", Instant.now()), "corr-rules");

        String bigAmountAccount = account();
        String bigAmountId = UUID.randomUUID().toString();
        sendRaw(KafkaTopics.TRANSFER_CREATED, bigAmountAccount,
                createdJson(bigAmountId, bigAmountAccount, "ACC-DEST", "10000000.01", Instant.now()), "corr-rules");

        assertDecision(consumeOne(KafkaTopics.TRANSFER_EVALUATED, sameAccountId), "REJECTED", "SAME_ACCOUNT");
        assertDecision(consumeOne(KafkaTopics.TRANSFER_EVALUATED, bigAmountId), "REJECTED", "AMOUNT_EXCEEDED");
        mockMvc.perform(get("/evaluations/{transferId}", bigAmountId))
                .andExpect(jsonPath("$.reason").value("AMOUNT_EXCEEDED"));
    }

    @Test
    void velocityRuleRejectsTheFourthTransferInsideTheWindowUsingEventTime() throws Exception {
        String fromAccount = account();
        // Horas del evento, no de procesamiento: se envían seguidos pero simulan 0, 1, 2, 3 y 70 s.
        Instant start = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        long[] offsets = {0, 1, 2, 3, 70};
        List<String> ids = new ArrayList<>();
        for (long offset : offsets) {
            String transferId = UUID.randomUUID().toString();
            ids.add(transferId);
            sendRaw(KafkaTopics.TRANSFER_CREATED, fromAccount,
                    createdJson(transferId, fromAccount, "ACC-DEST", "1000.00", start.plusSeconds(offset)),
                    "corr-velocity");
        }

        assertDecision(consumeOne(KafkaTopics.TRANSFER_EVALUATED, ids.get(0)), "APPROVED", null);
        assertDecision(consumeOne(KafkaTopics.TRANSFER_EVALUATED, ids.get(1)), "APPROVED", null);
        assertDecision(consumeOne(KafkaTopics.TRANSFER_EVALUATED, ids.get(2)), "APPROVED", null);
        assertDecision(consumeOne(KafkaTopics.TRANSFER_EVALUATED, ids.get(3)), "REJECTED", "VELOCITY_EXCEEDED");
        // A los 70 s la ventana es [10 s, 70 s): las cuatro anteriores quedan fuera.
        assertDecision(consumeOne(KafkaTopics.TRANSFER_EVALUATED, ids.get(4)), "APPROVED", null);
    }

    @Test
    void duplicateIsNotReevaluatedAndResendsTheStoredDecision() throws Exception {
        String fromAccount = account();
        String transferId = UUID.randomUUID().toString();
        Instant createdAt = Instant.now();
        sendRaw(KafkaTopics.TRANSFER_CREATED, fromAccount,
                createdJson(transferId, fromAccount, "ACC-DEST", "1000.00", createdAt), "corr-dup");
        // Mismo transferId pero ahora con la misma cuenta: si se reevaluara, saldría SAME_ACCOUNT.
        sendRaw(KafkaTopics.TRANSFER_CREATED, fromAccount,
                createdJson(transferId, fromAccount, fromAccount, "1000.00", createdAt), "corr-dup");

        List<ConsumerRecord<String, String>> records = consume(KafkaTopics.TRANSFER_EVALUATED,
                r -> transferId.equals(r.key()), 2);

        JsonNode first = jsonMapper.readTree(records.get(0).value());
        JsonNode second = jsonMapper.readTree(records.get(1).value());
        assertThat(first.get("decision").asString()).isEqualTo("APPROVED");
        assertThat(second.get("decision").asString()).isEqualTo("APPROVED");
        assertThat(second.get("eventId").asString()).isEqualTo(first.get("eventId").asString());
        assertThat(second.get("evaluatedAt").asString()).isEqualTo(first.get("evaluatedAt").asString());
        assertThat(evaluationRepository.findByTransferId(UUID.fromString(transferId))).isPresent();
        assertThat(evaluationRepository.countInWindow(fromAccount, createdAt.minusSeconds(1),
                createdAt.plusSeconds(1))).isEqualTo(1);
    }

    @Test
    void malformedEventGoesToDltWithoutRetries() {
        String key = account();
        sendRaw(KafkaTopics.TRANSFER_CREATED, key, "esto no es json", "corr-bad");

        ConsumerRecord<String, String> record =
                consume(KafkaTopics.TRANSFER_CREATED_DLT, r -> key.equals(r.key()), 1).getFirst();

        assertThat(record.value()).isEqualTo("esto no es json");
        assertThat(record.partition()).isEqualTo(partitionOf(key));
    }

    @Test
    void eventWithMissingFieldsGoesToDltWithoutRetries() throws Exception {
        String fromAccount = account();
        String transferId = UUID.randomUUID().toString();
        String event = """
                {"eventId":"%s","transferId":"%s","fromAccount":"%s","toAccount":"ACC-DEST",\
                "amount":null,"currency":"COP","createdAt":"%s"}"""
                .formatted(UUID.randomUUID(), transferId, fromAccount, Instant.now());
        Instant sentAt = Instant.now();
        sendRaw(KafkaTopics.TRANSFER_CREATED, fromAccount, event, "corr-invalid");

        ConsumerRecord<String, String> record =
                consume(KafkaTopics.TRANSFER_CREATED_DLT, r -> fromAccount.equals(r.key()), 1).getFirst();

        // Con reintentos tardaría al menos 7 s (1 s + 2 s + 4 s).
        assertThat(Duration.between(sentAt, Instant.ofEpochMilli(record.timestamp())))
                .isLessThan(Duration.ofSeconds(3));
        assertThat(jsonMapper.readTree(record.value()).get("transferId").asString()).isEqualTo(transferId);
        assertThat(evaluationRepository.findByTransferId(UUID.fromString(transferId))).isEmpty();
    }

    @Test
    void eventThatFailsProcessingGoesToDltAfterRetries() throws Exception {
        String fromAccount = account();
        String transferId = UUID.randomUUID().toString();
        // Monto válido como JSON pero que no cabe en NUMERIC(19,2): el insert falla en cada intento.
        Instant sentAt = Instant.now();
        sendRaw(KafkaTopics.TRANSFER_CREATED, fromAccount,
                createdJson(transferId, fromAccount, "ACC-DEST", "100000000000000000000.00", Instant.now()),
                "corr-retry");

        ConsumerRecord<String, String> record =
                consume(KafkaTopics.TRANSFER_CREATED_DLT, r -> fromAccount.equals(r.key()), 1).getFirst();

        assertThat(Duration.between(sentAt, Instant.ofEpochMilli(record.timestamp())))
                .isGreaterThanOrEqualTo(Duration.ofSeconds(7));
        assertThat(record.partition()).isEqualTo(partitionOf(fromAccount));
        assertThat(jsonMapper.readTree(record.value()).get("transferId").asString()).isEqualTo(transferId);
        assertThat(headerValue(record, CorrelationId.KAFKA_HEADER)).isEqualTo("corr-retry");
        assertThat(evaluationRepository.findByTransferId(UUID.fromString(transferId))).isEmpty();
    }

    @Test
    void queryErrorsReturnProblemDetail() throws Exception {
        mockMvc.perform(get("/evaluations/{transferId}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_PROBLEM_JSON_VALUE))
                .andExpect(jsonPath("$.status").value(404));

        mockMvc.perform(get("/evaluations/{transferId}", "no-es-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    private void assertDecision(ConsumerRecord<String, String> record, String decision, String reason) {
        JsonNode event = jsonMapper.readTree(record.value());
        assertThat(event.get("decision").asString()).isEqualTo(decision);
        if (reason == null) {
            assertThat(event.get("reason").isNull()).isTrue();
        } else {
            assertThat(event.get("reason").asString()).isEqualTo(reason);
        }
    }

    /** Cuenta única por prueba, dentro del máximo de 34 caracteres. */
    private static String account() {
        return "ACC-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Partición que asigna el particionador por defecto de Kafka (murmur2 sobre la clave). */
    private static int partitionOf(String key) {
        int partitions = 3;
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % partitions;
    }

    private static String createdJson(String transferId, String from, String to, String amount, Instant createdAt) {
        return """
                {"eventId":"%s","transferId":"%s","fromAccount":"%s","toAccount":"%s",\
                "amount":%s,"currency":"COP","createdAt":"%s"}"""
                .formatted(UUID.randomUUID(), transferId, from, to, amount, createdAt);
    }

    private static void sendRaw(String topic, String key, String value, String correlationId) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            record.headers().add(CorrelationId.KAFKA_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static ConsumerRecord<String, String> consumeOne(String topic, String key) {
        return consume(topic, r -> key.equals(r.key()), 1).getFirst();
    }

    /** Lee el topic desde el inicio hasta encontrar {@code count} mensajes que cumplan {@code match}. */
    private static List<ConsumerRecord<String, String>> consume(String topic,
                                                                Predicate<ConsumerRecord<String, String>> match,
                                                                int count) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(TIMEOUT);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (match.test(record)) {
                        found.add(record);
                        if (found.size() == count) {
                            return found;
                        }
                    }
                }
            }
        }
        throw new AssertionError("Llegaron " + found.size() + " de " + count + " mensajes esperados en " + topic);
    }

    private static String headerValue(ConsumerRecord<?, ?> record, String name) {
        return Optional.ofNullable(record.headers().lastHeader(name))
                .map(Header::value)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .orElse(null);
    }
}
