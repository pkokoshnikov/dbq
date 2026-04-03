package org.pak.qdb.runtime;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.pak.qdb.api.ConsumerConfig;
import org.pak.qdb.api.ProducerConfig;
import org.pak.qdb.pg.SchemaName;
import org.pak.qdb.api.SubscriptionId;
import org.junit.jupiter.api.BeforeAll;
import org.pak.qdb.model.MessageContainer;
import org.pak.qdb.model.MessageHistoryContainer;
import org.pak.qdb.model.Status;
import org.pak.qdb.policy.SimpleBlockingPolicy;
import org.pak.qdb.policy.SimpleNonRetryablePolicy;
import org.pak.qdb.policy.SimpleRetryablePolicy;
import org.pak.qdb.support.NoOpMessageConsumerTelemetry;
import org.pak.qdb.support.NoOpMessageContextPropagator;
import org.pak.qdb.support.StdMessageFactory;
import org.pak.qdb.support.StringFormatter;
import org.pak.qdb.pg.PgQueryService;
import org.pak.qdb.pg.PgSchemaSqlGenerator;
import org.pak.qdb.pg.PgTableManager;
import org.pak.qdb.pg.jsonb.JsonbConverter;
import org.pak.qdb.spring.SpringPersistenceService;
import org.pak.qdb.spring.SpringTransactionService;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.IOException;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.util.Optional.ofNullable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.qdb.runtime.TestMessage.QUEUE_NAME;

public class BaseIntegrationTest {
    static final String QUEUE_TABLE = QUEUE_NAME.name().replace("-", "_");
    static SubscriptionId SUBSCRIPTION_NAME_1 = new SubscriptionId("test-subscription-one");
    static String SUBSCRIPTION_TABLE_1 = SUBSCRIPTION_NAME_1.id().replace("-", "_");
    static String SUBSCRIPTION_TABLE_1_HISTORY = SUBSCRIPTION_NAME_1.id().replace("-", "_") + "_history";
    static SubscriptionId SUBSCRIPTION_NAME_2 = new SubscriptionId("test-subscription-two");
    static String SUBSCRIPTION_TABLE_2 = SUBSCRIPTION_NAME_2.id().replace("-", "_");
    static String SUBSCRIPTION_TABLE_2_HISTORY = SUBSCRIPTION_NAME_2.id().replace("-", "_") + "_history";
    static SchemaName TEST_SCHEMA = new SchemaName("public");
    static String TEST_VALUE = "test-value";
    static String TEST_EXCEPTION_MESSAGE = "test-exception-payload";
    PgQueryService pgQueryService;
    PgTableManager tableManager;
    PgSchemaSqlGenerator schemaSqlGenerator;
    static StringFormatter formatter = new StringFormatter();
    static Network network = Network.newNetwork();
    static String jdbcUrl;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:18-alpine"))
            .withNetwork(network)
            .withNetworkAliases("postgres");
    @Container
    static ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);
    static Proxy postgresqlProxy;
    JdbcTemplate jdbcTemplate;
    JsonbConverter jsonbConverter;
    SpringTransactionService springTransactionService;
    QueueProcessorFactory.QueueProcessorFactoryBuilder<TestMessage> queueProcessorFactory;
    ProducerFactory.ProducerFactoryBuilder<TestMessage> producerFactory;
    DataSource dataSource;
    SpringPersistenceService persistenceService;

    @BeforeAll
    static void beforeAll() throws IOException {
        var toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        postgresqlProxy = toxiproxyClient.createProxy("postgresql", "0.0.0.0:8666", "postgres:5432");
        jdbcUrl = "jdbc:postgresql://%s:%d/%s".formatted(toxiproxy.getHost(), toxiproxy.getMappedPort(8666),
                postgres.getDatabaseName());
    }

    static JdbcTemplate setupJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    static DataSource setupDatasource() {
        var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setDatabaseName(postgres.getDatabaseName());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        return dataSource;
    }

    static SpringTransactionService setupSpringTransactionService(DataSource dataSource) {
        return new SpringTransactionService(new TransactionTemplate(
                new JdbcTransactionManager(dataSource) {}));
    }

    static SpringPersistenceService setupPersistenceService(JdbcTemplate jdbcTemplate) {
        return new SpringPersistenceService(jdbcTemplate);
    }

    static PgQueryService setupQueryService(SpringPersistenceService persistenceService, JsonbConverter jsonbConverter) {
        return new PgQueryService(persistenceService, TEST_SCHEMA, jsonbConverter);
    }

    static JsonbConverter setupJsonbConverter() {
        var jsonbConverter = new JsonbConverter();
        jsonbConverter.registerType(QUEUE_NAME.name(), TestMessage.class);
        return jsonbConverter;
    }

    static PgSchemaSqlGenerator setupSchemaSqlGenerator() {
        return new PgSchemaSqlGenerator(TEST_SCHEMA);
    }

    static ProducerFactory.ProducerFactoryBuilder<TestMessage> setupProducerFactory(
            PgQueryService pgQueryService
    ) {
        return ProducerFactory.<TestMessage>builder()
                .producerConfig(ProducerConfig.<TestMessage>builder()
                        .properties(ProducerConfig.Properties.builder()
                                .storageDays(30)
                                .build())
                        .queueName(QUEUE_NAME)
                        .clazz(TestMessage.class)
                        .messageContextPropagator(new NoOpMessageContextPropagator())
                        .build())
                .messageFactory(new StdMessageFactory())
                .queryService(pgQueryService);
    }

    static QueueProcessorFactory.QueueProcessorFactoryBuilder<TestMessage> setupQueueProcessorFactory(
            PgQueryService pgQueryService,
            SpringTransactionService springTransactionService
    ) {
        return QueueProcessorFactory.<TestMessage>builder()
                .messageFactory(new StdMessageFactory())
                .messageHandler(testMessage -> {})
                .queryService(pgQueryService)
                .transactionService(springTransactionService)
                .retryablePolicy(new SimpleRetryablePolicy())
                .blockingPolicy(new SimpleBlockingPolicy())
                .nonRetryablePolicy(new SimpleNonRetryablePolicy())
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_1)
                .messageContextPropagator(new NoOpMessageContextPropagator())
                .messageConsumerTelemetry(new NoOpMessageConsumerTelemetry())
                .properties(ConsumerConfig.Properties.builder().build());
    }

    static PgTableManager setupTableManager(PgQueryService pgQueryService) {
        return new PgTableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
    }

    void createQueueTable() {
        jdbcTemplate.execute(schemaSqlGenerator.createQueueTable(QUEUE_NAME));
    }

    void createSubscriptionTable(SubscriptionId subscriptionId) {
        jdbcTemplate.execute(schemaSqlGenerator.createSubscriptionTable(QUEUE_NAME, subscriptionId));
    }

    void clearTables() {
        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", SUBSCRIPTION_TABLE_1)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}_history",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", SUBSCRIPTION_TABLE_1)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", SUBSCRIPTION_TABLE_2)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}_history",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", SUBSCRIPTION_TABLE_2)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${queueTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "queueTable", QUEUE_TABLE)));
    }

    static MessageContainer<TestMessage> hasSize1AndGetFirst(List<MessageContainer<TestMessage>> testMessageContainers) {
        assertThat(testMessageContainers).hasSize(1);
        return testMessageContainers.get(0);
    }

    static MessageHistoryContainer<TestMessage> hasSize1AndGetFirstHistory(List<MessageHistoryContainer<TestMessage>> testMessageContainers) {
        assertThat(testMessageContainers).hasSize(1);
        return testMessageContainers.get(0);
    }

    List<MessageContainer<TestMessage>> selectTestMessages(SubscriptionId subscriptionId) {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.payload, m.headers, m.originated_at, m.key
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", subscriptionId.id().replace("-", "_"),
                        "queueTable", "test_message"));

        return jdbcTemplate.query(query,
                (rs, rowNum) -> new MessageContainer<>(
                        rs.getObject("id", BigInteger.class),
                        rs.getObject("message_id", BigInteger.class),
                        rs.getString("key"),
                        rs.getInt("attempt"),
                        ofNullable(rs.getObject("execute_after", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        ofNullable(rs.getObject("created_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        ofNullable(rs.getObject("updated_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        ofNullable(rs.getObject("originated_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        jsonbConverter.toJsonb(rs.getObject("payload", PGobject.class)),
                        jsonbConverter.toStringMap(rs.getObject("headers", PGobject.class)),
                        rs.getString("error_message"),
                        rs.getString("stack_trace")));
    }

    List<MessageHistoryContainer<TestMessage>> selectTestMessagesFromHistory(SubscriptionId subscriptionId) {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.status, s.error_message, s.stack_trace, s.created_at, m.payload
                        FROM ${schema}.${subscriptionTableHistory} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTableHistory", subscriptionId.id().replace("-", "_") + "_history",
                        "queueTable", "test_message"));

        return jdbcTemplate.query(query,
                (rs, rowNum) -> new MessageHistoryContainer<>(
                        rs.getObject("id", BigInteger.class),
                        rs.getInt("attempt"),
                        ofNullable(rs.getObject("created_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        Status.valueOf(rs.getString("status")),
                        jsonbConverter.toJsonb(rs.getObject("payload", PGobject.class)),
                        rs.getString("error_message"),
                        rs.getString("stack_trace")));
    }

    void assertPartitions(String tableName, List<String> partitions) {
        var params = Map.of("table", tableName);
        partitions.forEach(partition -> {
            var matches = Pattern.compile(formatter.execute("${table}_\\d{4}_\\d{2}_\\d{2}", params))
                    .matcher(partition)
                    .matches();
            assertThat(matches).isTrue();
        });
    }

    List<String> selectPartitions(String tableName) {
        Map<String, String> formatParams = Map.of("schema", TEST_SCHEMA.value(),
                "table", tableName);
        var query = formatter.execute("""
                        SELECT inhrelid::regclass AS partition
                        FROM   pg_catalog.pg_inherits
                        WHERE  inhparent = '${schema}.${table}'::regclass;""",
                formatParams);
        return jdbcTemplate.query(query, (rs, rowNum) -> rs.getString("partition"));
    }


    static class BlockingApplicationException extends RuntimeException {
        public BlockingApplicationException(String message) {
            super(message);
        }
    }

    static class RetryableApplicationException extends RuntimeException {
        public RetryableApplicationException(String message) {
            super(message);
        }
    }

    static class NonRetryableApplicationException extends RuntimeException {
        public NonRetryableApplicationException(String message) {
            super(message);
        }
    }
}
