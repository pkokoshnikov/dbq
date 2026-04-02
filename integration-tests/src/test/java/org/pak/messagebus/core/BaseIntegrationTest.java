package org.pak.messagebus.core;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.BeforeAll;
import org.pak.messagebus.pg.PgQueryService;
import org.pak.messagebus.pg.jsonb.JsonbConverter;
import org.pak.messagebus.spring.SpringPersistenceService;
import org.pak.messagebus.spring.SpringTransactionService;
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
import static org.pak.messagebus.core.TestMessage.MESSAGE_NAME;

public class BaseIntegrationTest {
    static final String MESSAGE_TABLE = MESSAGE_NAME.name().replace("-", "_");
    static SubscriptionName SUBSCRIPTION_NAME_1 = new SubscriptionName("test-subscription-one");
    static String SUBSCRIPTION_TABLE_1 = SUBSCRIPTION_NAME_1.name().replace("-", "_");
    static String SUBSCRIPTION_TABLE_1_HISTORY = SUBSCRIPTION_NAME_1.name().replace("-", "_") + "_history";
    static SubscriptionName SUBSCRIPTION_NAME_2 = new SubscriptionName("test-subscription-two");
    static String SUBSCRIPTION_TABLE_2 = SUBSCRIPTION_NAME_2.name().replace("-", "_");
    static String SUBSCRIPTION_TABLE_2_HISTORY = SUBSCRIPTION_NAME_2.name().replace("-", "_") + "_history";
    static SchemaName TEST_SCHEMA = new SchemaName("public");
    static String TEST_VALUE = "test-value";
    static String TEST_EXCEPTION_MESSAGE = "test-exception-payload";
    PgQueryService pgQueryService;
    TableManager tableManager;
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
    MessageProcessorFactory.MessageProcessorFactoryBuilder<TestMessage> messageProcessorFactory;
    MessagePublisherFactory.MessagePublisherFactoryBuilder<TestMessage> messagePublisherFactory;
    QueueMessagePublisherFactory.QueueMessagePublisherFactoryBuilder<TestMessage> queueMessagePublisherFactory;
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
        jsonbConverter.registerType(MESSAGE_NAME.name(), TestMessage.class);
        return jsonbConverter;
    }

    static MessagePublisherFactory.MessagePublisherFactoryBuilder<TestMessage> setupMessagePublisherFactory(
            TableManager tableManager,
            PgQueryService pgQueryService
    ) {
        return MessagePublisherFactory.<TestMessage>builder()
                .publisherConfig(PublisherConfig.<TestMessage>builder()
                        .properties(PublisherConfig.Properties.builder()
                                .storageDays(30)
                                .build())
                        .messageName(MESSAGE_NAME)
                        .clazz(TestMessage.class)
                        .traceIdExtractor(new NullTraceIdExtractor<>())
                        .build())
                .messageFactory(new StdMessageFactory())
                .tableManager(tableManager)
                .queryService(pgQueryService);
    }

    static QueueMessagePublisherFactory.QueueMessagePublisherFactoryBuilder<TestMessage> setupQueueMessagePublisherFactory(
            TableManager tableManager,
            PgQueryService pgQueryService,
            SpringTransactionService transactionService
    ) {
        return QueueMessagePublisherFactory.<TestMessage>builder()
                .publisherConfig(PublisherConfig.<TestMessage>builder()
                        .properties(PublisherConfig.Properties.builder()
                                .storageDays(30)
                                .build())
                        .messageName(MESSAGE_NAME)
                        .clazz(TestMessage.class)
                        .traceIdExtractor(new NullTraceIdExtractor<>())
                        .build())
                .messageFactory(new StdMessageFactory())
                .transactionService(transactionService)
                .tableManager(tableManager)
                .queryService(pgQueryService);
    }

    static MessageProcessorFactory.MessageProcessorFactoryBuilder<TestMessage> setupMessageProcessorFactory(
            PgQueryService pgQueryService,
            SpringTransactionService springTransactionService
    ) {
        return MessageProcessorFactory.<TestMessage>builder()
                .messageFactory(new StdMessageFactory())
                .messageListener(testMessage -> {})
                .queryService(pgQueryService)
                .transactionService(springTransactionService)
                .retryablePolicy(new StdRetryablePolicy())
                .blockingPolicy(new StdBlockingPolicy())
                .nonRetryablePolicy(new StdNonRetryablePolicy())
                .messageName(MESSAGE_NAME)
                .subscriptionName(SUBSCRIPTION_NAME_1)
                .traceIdExtractor(object -> null)
                .properties(SubscriberConfig.Properties.builder().build());
    }

    static TableManager setupTableManager(PgQueryService pgQueryService) {
        return new TableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
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

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${messageTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "messageTable", MESSAGE_TABLE)));
    }

    static MessageContainer<TestMessage> hasSize1AndGetFirst(List<MessageContainer<TestMessage>> testMessageContainers) {
        assertThat(testMessageContainers).hasSize(1);
        return testMessageContainers.get(0);
    }

    static MessageHistoryContainer<TestMessage> hasSize1AndGetFirstHistory(List<MessageHistoryContainer<TestMessage>> testMessageContainers) {
        assertThat(testMessageContainers).hasSize(1);
        return testMessageContainers.get(0);
    }

    List<MessageContainer<TestMessage>> selectTestMessages(SubscriptionName subscriptionName) {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.payload, m.originated_at, m.key
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${messageTable} m ON s.message_id = m.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", subscriptionName.name().replace("-", "_"),
                        "messageTable", "test_message"));

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
                        rs.getString("error_message"),
                        rs.getString("stack_trace")));
    }

    List<MessageHistoryContainer<TestMessage>> selectTestMessagesFromHistory(SubscriptionName subscriptionName) {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.status, s.error_message, s.stack_trace, s.created_at, m.payload
                        FROM ${schema}.${subscriptionTableHistory} s JOIN ${schema}.${messageTable} m ON s.message_id = m.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTableHistory", subscriptionName.name().replace("-", "_") + "_history",
                        "messageTable", "test_message"));

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
