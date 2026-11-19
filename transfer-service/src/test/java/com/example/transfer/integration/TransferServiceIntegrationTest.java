package com.example.transfer.integration;

import com.example.transfer.config.CorrelationId;
import com.example.transfer.config.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "transfer.outbox.poll-interval=200ms")
@AutoConfigureMockMvc
class TransferServiceIntegrationTest {

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

    @Test
    void createPublishesTransferCreatedThroughOutboxWithCorrelationId() throws Exception {
        String fromAccount = account();
        MvcResult result = mockMvc.perform(post("/transfers")
                        .header(CorrelationId.HTTP_HEADER, "corr-it-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createBody(fromAccount, "ACC-DEST", "150000.00", "COP")))
                .andExpect(status().isAccepted())
                .andExpect(header().string(CorrelationId.HTTP_HEADER, "corr-it-1"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        String transferId = jsonMapper.readTree(result.getResponse().getContentAsString()).get("id").asString();

        ConsumerRecord<String, String> record =
                consumeUntil(KafkaTopics.TRANSFER_CREATED, r -> r.value().contains(transferId));

        assertThat(record.key()).isEqualTo(fromAccount);
        // El monto conserva sus 2 decimales (PostgreSQL devuelve el JSONB con espacios tras ':').
        assertThat(record.value()).containsPattern("\"amount\":\\s*150000\\.00[,}]");
        JsonNode event = jsonMapper.readTree(record.value());
        assertThat(event.get("transferId").asString()).isEqualTo(transferId);
        assertThat(event.get("eventId").asString()).isNotBlank();
        assertThat(event.get("fromAccount").asString()).isEqualTo(fromAccount);
        assertThat(event.get("toAccount").asString()).isEqualTo("ACC-DEST");
        assertThat(event.get("amount").decimalValue()).isEqualByComparingTo("150000.00");
        assertThat(event.get("currency").asString()).isEqualTo("COP");
        assertThat(event.get("createdAt").asString()).isNotBlank();
        assertThat(headerValue(record, CorrelationId.KAFKA_HEADER)).isEqualTo("corr-it-1");
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();
    }

    @Test
    void evaluatedEventUpdatesTransferAndDuplicatesAreIgnored() throws Exception {
        String transferId = createTransfer(account());

        sendRaw(KafkaTopics.TRANSFER_EVALUATED, transferId, evaluatedJson(transferId, "APPROVED", null));
        awaitTrue(() -> "COMPLETED".equals(transferStatus(transferId)));

        // Un duplicado (aunque traiga otra decisión) no cambia una transferencia que ya no está en PENDING.
        sendRaw(KafkaTopics.TRANSFER_EVALUATED, transferId, evaluatedJson(transferId, "REJECTED", "SAME_ACCOUNT"));
        String marker = createTransfer(account());
        sendRaw(KafkaTopics.TRANSFER_EVALUATED, transferId, evaluatedJson(marker, "REJECTED", "AMOUNT_EXCEEDED"));
        // El marcador va con la misma clave (misma partición, procesado después del duplicado).
        awaitTrue(() -> "REJECTED".equals(transferStatus(marker)));

        mockMvc.perform(get("/transfers/{id}", transferId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.reason").doesNotExist());
        mockMvc.perform(get("/transfers/{id}", marker))
                .andExpect(jsonPath("$.reason").value("AMOUNT_EXCEEDED"));
    }

    @Test
    void malformedEvaluatedEventGoesToDltWithoutRetries() {
        String key = UUID.randomUUID().toString();
        sendRaw(KafkaTopics.TRANSFER_EVALUATED, key, "esto no es json");

        ConsumerRecord<String, String> record =
                consumeUntil(KafkaTopics.TRANSFER_EVALUATED_DLT, r -> key.equals(r.key()));

        assertThat(record.value()).isEqualTo("esto no es json");
        assertThat(record.partition()).isEqualTo(partitionOf(key));
    }

    @Test
    void eventThatFailsProcessingGoesToDltAfterRetries() throws Exception {
        String transferId = createTransfer(account());
        // JSON válido pero sin decisión: falla al procesarse, se reintenta 3 veces (1 s, 2 s, 4 s) y va a la DLT.
        String event = """
                {"eventId":"%s","transferId":"%s","decision":null,"reason":null,"evaluatedAt":"%s"}"""
                .formatted(UUID.randomUUID(), transferId, Instant.now());
        Instant sentAt = Instant.now();
        sendRaw(KafkaTopics.TRANSFER_EVALUATED, transferId, event);

        ConsumerRecord<String, String> record =
                consumeUntil(KafkaTopics.TRANSFER_EVALUATED_DLT, r -> transferId.equals(r.key()));

        assertThat(Duration.between(sentAt, Instant.ofEpochMilli(record.timestamp())))
                .isGreaterThanOrEqualTo(Duration.ofSeconds(7));
        assertThat(jsonMapper.readTree(record.value()).get("transferId").asString()).isEqualTo(transferId);
        assertThat(headerValue(record, CorrelationId.KAFKA_HEADER)).isEqualTo("corr-test");
        assertThat(transferStatus(transferId)).isEqualTo("PENDING");
    }

    @Test
    void invalidRequestsReturnProblemDetail() throws Exception {
        mockMvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON)
                        .content(createBody("ACC-1", "ACC-2", "1000.00", "USD")))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_PROBLEM_JSON_VALUE))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.errors.currency").exists());

        mockMvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON)
                        .content(createBody("ACC-1", "ACC-2", "10.123", "COP")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.amount").exists());

        mockMvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON)
                        .content(createBody("ACC-1", "ACC-2", "0", "COP")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.amount").exists());

        mockMvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON)
                        .content(createBody(" ", "ACC-2", "10", "COP")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.fromAccount").exists());

        mockMvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON).content("{no json"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(get("/transfers/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));

        mockMvc.perform(get("/transfers"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void sameAccountIsAcceptedByTheApi() throws Exception {
        mockMvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON)
                        .content(createBody("ACC-SAME", "ACC-SAME", "10.00", "COP")))
                .andExpect(status().isAccepted());
    }

    @Test
    void listsTransfersOfAnAccountPaginated() throws Exception {
        String fromAccount = account();
        createTransfer(fromAccount);
        createTransfer(fromAccount);
        createTransfer(fromAccount);

        mockMvc.perform(get("/transfers").param("fromAccount", fromAccount).param("page", "0").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(2))
                .andExpect(jsonPath("$.content[0].fromAccount").value(fromAccount))
                .andExpect(jsonPath("$.totalElements").value(3))
                .andExpect(jsonPath("$.totalPages").value(2));
    }

    private String createTransfer(String fromAccount) throws Exception {
        MvcResult result = mockMvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON)
                        .content(createBody(fromAccount, "ACC-DEST", "1000.00", "COP")))
                .andExpect(status().isAccepted())
                .andReturn();
        return jsonMapper.readTree(result.getResponse().getContentAsString()).get("id").asString();
    }

    private String transferStatus(String transferId) {
        try {
            MvcResult result = mockMvc.perform(get("/transfers/{id}", transferId)).andReturn();
            return jsonMapper.readTree(result.getResponse().getContentAsString()).get("status").asString();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /** Cuenta única por prueba, dentro del máximo de 34 caracteres. */
    private static String account() {
        return "ACC-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Partición que asigna el particionador por defecto de Kafka (murmur2 sobre la clave). */
    private static int partitionOf(String key) {
        int partitions = 3;
        return org.apache.kafka.common.utils.Utils.toPositive(
                org.apache.kafka.common.utils.Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % partitions;
    }

    private static String createBody(String from, String to, String amount, String currency) {
        return """
                {"fromAccount":"%s","toAccount":"%s","amount":%s,"currency":"%s"}"""
                .formatted(from, to, amount, currency);
    }

    private static String evaluatedJson(String transferId, String decision, String reason) {
        String reasonJson = reason == null ? "null" : "\"" + reason + "\"";
        return """
                {"eventId":"%s","transferId":"%s","decision":"%s","reason":%s,"evaluatedAt":"%s"}"""
                .formatted(UUID.randomUUID(), transferId, decision, reasonJson, Instant.now());
    }

    private static void sendRaw(String topic, String key, String value) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            record.headers().add(CorrelationId.KAFKA_HEADER, "corr-test".getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static ConsumerRecord<String, String> consumeUntil(String topic,
                                                               Predicate<ConsumerRecord<String, String>> match) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(TIMEOUT);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (match.test(record)) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No llegó el mensaje esperado al topic " + topic);
    }

    private static String headerValue(ConsumerRecord<?, ?> record, String name) {
        return Optional.ofNullable(record.headers().lastHeader(name))
                .map(Header::value)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .orElse(null);
    }

    private static void awaitTrue(Supplier<Boolean> condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            if (condition.get()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("La condición no se cumplió en " + TIMEOUT);
    }
}
