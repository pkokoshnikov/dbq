package org.pak.dbq.internal;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.BeforeAll;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.api.policy.SimpleBlockingPolicy;
import org.pak.dbq.api.policy.SimpleNonRetryablePolicy;
import org.pak.dbq.api.policy.SimpleRetryablePolicy;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.persistence.MessageHistoryContainer;
import org.pak.dbq.internal.persistence.Status;
import org.pak.dbq.internal.support.NoOpMessageConsumerTelemetry;
import org.pak.dbq.internal.support.NoOpMessageContextPropagator;
import org.pak.dbq.internal.support.SimpleMessageFactory;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;
import org.pak.dbq.pg.PgTableManager;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.pg.jsonb.JsonbConverter;
import org.pak.dbq.spring.SpringPersistenceService;
import org.pak.dbq.spring.SpringTransactionService;
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
import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;

public class BaseIntegrationTest {
    protected static final String QUEUE_TABLE = QUEUE_NAME.name().replace("-", "_");
    protected static final SubscriptionId SUBSCRIPTION_NAME_1 = new SubscriptionId("test-subscription-one");
    protected static final String SUBSCRIPTION_TABLE_1 = SUBSCRIPTION_NAME_1.id().replace("-", "_");
    protected static final String SUBSCRIPTION_TABLE_1_HISTORY = SUBSCRIPTION_NAME_1.id().replace("-", "_") + "_history";
    protected static final String SUBSCRIPTION_TABLE_1_KEY_LOCK = SUBSCRIPTION_NAME_1.id().replace("-", "_") + "_key_lock";
    protected static final SubscriptionId SUBSCRIPTION_NAME_2 = new SubscriptionId("test-subscription-two");
    protected static final String SUBSCRIPTION_TABLE_2 = SUBSCRIPTION_NAME_2.id().replace("-", "_");
    protected static final String SUBSCRIPTION_TABLE_2_HISTORY = SUBSCRIPTION_NAME_2.id().replace("-", "_") + "_history";
    protected static final String SUBSCRIPTION_TABLE_2_KEY_LOCK = SUBSCRIPTION_NAME_2.id().replace("-", "_") + "_key_lock";
    protected static final SchemaName TEST_SCHEMA = new SchemaName("public");
    protected static final String TEST_VALUE = "test-value";
    protected static final String TEST_EXCEPTION_MESSAGE = "test-exception-payload";
    protected PgQueryService pgQueryService;
    protected PgTableManager tableManager;
    protected static final StringFormatter formatter = new StringFormatter();
    protected static final Network network = Network.newNetwork();
    protected static String jdbcUrl;

    @Container
    protected static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:18-alpine"))
            .withNetwork(network)
            .withNetworkAliases("postgres");
    @Container
    protected static ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);
    protected static Proxy postgresqlProxy;
    protected JdbcTemplate jdbcTemplate;
    protected JsonbConverter jsonbConverter;
    protected SpringTransactionService springTransactionService;
    protected ConsumerFactory.ConsumerFactoryBuilder<TestMessage> consumerFactoryBuilder;
    protected ProducerFactory.ProducerFactoryBuilder<TestMessage> producerFactory;
    protected DataSource dataSource;
    protected SpringPersistenceService persistenceService;

    @BeforeAll
    static void beforeAll() throws IOException {
        var toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        postgresqlProxy = toxiproxyClient.createProxy("postgresql", "0.0.0.0:8666", "postgres:5432");
        jdbcUrl = "jdbc:postgresql://%s:%d/%s".formatted(toxiproxy.getHost(), toxiproxy.getMappedPort(8666),
                postgres.getDatabaseName());
    }

    protected static JdbcTemplate setupJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    protected static DataSource setupDatasource() {
        var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setDatabaseName(postgres.getDatabaseName());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        return dataSource;
    }

    protected static SpringTransactionService setupSpringTransactionService(DataSource dataSource) {
        return new SpringTransactionService(new TransactionTemplate(
                new JdbcTransactionManager(dataSource) {}));
    }

    protected static SpringPersistenceService setupPersistenceService(JdbcTemplate jdbcTemplate) {
        return new SpringPersistenceService(jdbcTemplate);
    }

    protected static PgQueryService setupQueryService(
            SpringPersistenceService persistenceService,
            JsonbConverter jsonbConverter
    ) {
        return new PgQueryService(persistenceService, TEST_SCHEMA, jsonbConverter);
    }

    protected static JsonbConverter setupJsonbConverter() {
        var jsonbConverter = new JsonbConverter();
        jsonbConverter.registerType(QUEUE_NAME.name(), TestMessage.class);
        return jsonbConverter;
    }

    protected static ProducerFactory.ProducerFactoryBuilder<TestMessage> setupProducerFactory(
            PgQueryService pgQueryService
    ) {
        return ProducerFactory.<TestMessage>builder()
                .producerConfig(ProducerConfig.<TestMessage>builder()
                        .queueName(QUEUE_NAME)
                        .clazz(TestMessage.class)
                        .messageContextPropagator(new NoOpMessageContextPropagator())
                        .build())
                .messageFactory(new SimpleMessageFactory())
                .queryServiceFactory(pgQueryService);
    }

    protected static ConsumerFactory.ConsumerFactoryBuilder<TestMessage> setupQueueProcessorFactory(
            PgQueryService pgQueryService,
            SpringTransactionService springTransactionService
    ) {
        return ConsumerFactory.<TestMessage>builder()
                .messageFactory(new SimpleMessageFactory())
                .messageHandler(testMessage -> {})
                .queryServiceFactory(pgQueryService)
                .transactionService(springTransactionService)
                .retryablePolicy(new SimpleRetryablePolicy())
                .blockingPolicy(new SimpleBlockingPolicy())
                .nonRetryablePolicy(new SimpleNonRetryablePolicy())
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_1)
                .messageContextPropagator(new NoOpMessageContextPropagator())
                .messageConsumerTelemetry(new NoOpMessageConsumerTelemetry())
                .properties(ConsumerConfig.Properties.builder()
                        .historyEnabled(true)
                        .build());
    }

    protected static PgTableManager setupTableManager(PgQueryService pgQueryService) {
        return new PgTableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
    }

    protected void createQueueTable() throws Exception {
        pgQueryService.createQueueTable(QUEUE_NAME);
    }

    protected void createSubscriptionTable(SubscriptionId subscriptionId) throws Exception {
        createSubscriptionTable(subscriptionId, false, false);
    }

    protected void createSubscriptionTable(SubscriptionId subscriptionId, boolean historyEnabled) throws Exception {
        createSubscriptionTable(subscriptionId, historyEnabled, false);
    }

    protected void createSubscriptionTable(
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) throws Exception {
        pgQueryService.createSubscriptionTable(QUEUE_NAME, subscriptionId, historyEnabled, serializedByKey);
    }

    protected void clearTables() {
        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", SUBSCRIPTION_TABLE_1)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}_history",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", SUBSCRIPTION_TABLE_1)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${keyLockTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "keyLockTable", SUBSCRIPTION_TABLE_1_KEY_LOCK)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", SUBSCRIPTION_TABLE_2)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}_history",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", SUBSCRIPTION_TABLE_2)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${keyLockTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "keyLockTable", SUBSCRIPTION_TABLE_2_KEY_LOCK)));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${queueTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "queueTable", QUEUE_TABLE)));
    }

    protected static MessageContainer<TestMessage> hasSize1AndGetFirst(
            List<MessageContainer<TestMessage>> testMessageContainers
    ) {
        assertThat(testMessageContainers).hasSize(1);
        return testMessageContainers.get(0);
    }

    protected static MessageHistoryContainer<TestMessage> hasSize1AndGetFirstHistory(
            List<MessageHistoryContainer<TestMessage>> testMessageContainers
    ) {
        assertThat(testMessageContainers).hasSize(1);
        return testMessageContainers.get(0);
    }

    protected List<MessageContainer<TestMessage>> selectTestMessages(SubscriptionId subscriptionId) {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.payload, m.headers, m.originated_at, m.key
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", subscriptionId.id().replace("-", "_"),
                        "queueTable", "test_message"));

        return jdbcTemplate.query(query,
                (rs, rowNum) -> {
                    try {
                        return new MessageContainer<>(
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
                                jsonbConverter.fromPGobject(rs.getObject("payload", PGobject.class)),
                                jsonbConverter.fromPGHeaders(rs.getObject("headers", PGobject.class)),
                                rs.getString("error_message"),
                                rs.getString("stack_trace"));
                    } catch (DbqException e) {
                        return sneakyThrow(e);
                    }
                });
    }

    protected List<MessageHistoryContainer<TestMessage>> selectTestMessagesFromHistory(
            SubscriptionId subscriptionId
    ) {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.status, s.error_message, s.stack_trace, s.created_at, m.payload
                        FROM ${schema}.${subscriptionTableHistory} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTableHistory", subscriptionId.id().replace("-", "_") + "_history",
                        "queueTable", "test_message"));

        return jdbcTemplate.query(query,
                (rs, rowNum) -> {
                    try {
                        return new MessageHistoryContainer<>(
                                rs.getObject("id", BigInteger.class),
                                rs.getInt("attempt"),
                                ofNullable(rs.getObject("created_at", OffsetDateTime.class))
                                        .map(OffsetDateTime::toInstant).orElse(null),
                                Status.valueOf(rs.getString("status")),
                                jsonbConverter.fromPGobject(rs.getObject("payload", PGobject.class)),
                                rs.getString("error_message"),
                                rs.getString("stack_trace"));
                    } catch (DbqException e) {
                        return sneakyThrow(e);
                    }
                });
    }

    protected void assertPartitions(String tableName, List<String> partitions) {
        var params = Map.of("table", tableName);
        partitions.forEach(partition -> {
            var matches = Pattern.compile(formatter.execute("${table}_\\d{4}_\\d{2}_\\d{2}", params))
                    .matcher(partition)
                    .matches();
            assertThat(matches).isTrue();
        });
    }

    protected List<String> selectPartitions(String tableName) {
        Map<String, String> formatParams = Map.of("schema", TEST_SCHEMA.value(),
                "table", tableName);
        var query = formatter.execute("""
                        SELECT inhrelid::regclass AS partition
                        FROM   pg_catalog.pg_inherits
                        WHERE  inhparent = '${schema}.${table}'::regclass;""",
                formatParams);
        return jdbcTemplate.query(query, (rs, rowNum) -> rs.getString("partition"));
    }

    @SuppressWarnings("unchecked")
    private static <T, E extends Throwable> T sneakyThrow(Throwable throwable) throws E {
        throw (E) throwable;
    }


    protected static class BlockingApplicationException extends RuntimeException {
        public BlockingApplicationException(String message) {
            super(message);
        }
    }

    protected static class RetryableApplicationException extends RuntimeException {
        public RetryableApplicationException(String message) {
            super(message);
        }
    }

    protected static class NonRetryableApplicationException extends RuntimeException {
        public NonRetryableApplicationException(String message) {
            super(message);
        }
    }
}
