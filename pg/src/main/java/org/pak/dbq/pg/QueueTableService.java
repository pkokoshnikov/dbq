package org.pak.dbq.pg;

import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.spi.PersistenceService;

import java.util.Map;

@Slf4j
public class QueueTableService {
    private static final StringFormatter STATIC_FORMATTER = new StringFormatter();

    private final PersistenceService persistenceService;
    private final SchemaName schemaName;

    public QueueTableService(PersistenceService persistenceService, SchemaName schemaName) {
        this.persistenceService = persistenceService;
        this.schemaName = schemaName;
    }

    public void createQueueTable(QueueName queueName) throws DbqException {
        persistenceService.execute(createQueueTableSql(schemaName, queueName));
    }

    public void createSubscriptionTable(
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) throws DbqException {
        persistenceService.execute(createSubscriptionTableSql(
                schemaName,
                queueName,
                subscriptionId,
                historyEnabled,
                serializedByKey));
    }

    public static String createQueueTableSql(SchemaName schemaName, QueueName queueName) {
        return STATIC_FORMATTER.execute("""
                CREATE TABLE IF NOT EXISTS ${schema}.${queueTable} (
                    id BIGSERIAL,
                    key TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    originated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
                    payload JSONB NOT NULL,
                    PRIMARY KEY (id, originated_at)
                ) PARTITION BY RANGE (originated_at);
                CREATE INDEX IF NOT EXISTS ${queueTable}_created_at_idx ON ${schema}.${queueTable}(created_at);
                CREATE UNIQUE INDEX IF NOT EXISTS ${queueTable}_message_key_idx ON ${schema}.${queueTable}(originated_at, key);
                """, Map.of(
                "schema", schemaName.value(),
                "queueTable", TableNames.queueTableName(queueName))
        );
    }

    public static String createSubscriptionTableSql(
            SchemaName schemaName,
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) {
        var params = Map.of(
                "schema", schemaName.value(),
                "queueTable", TableNames.queueTableName(queueName),
                "subscriptionTable", TableNames.subscriptionTableName(subscriptionId),
                "subscriptionHistoryTable", TableNames.subscriptionHistoryTableName(subscriptionId),
                "subscriptionKeyLockTable", TableNames.subscriptionKeyLockTableName(subscriptionId),
                "insertTrigger", TableNames.subscriptionTableName(subscriptionId) + "_insert_trigger",
                "insertFunction", TableNames.subscriptionTableName(subscriptionId) + "_insert_function()",
                "subscriptionKeyIndex", TableNames.subscriptionTableName(subscriptionId) + "_key_idx");

        var sql = new StringBuilder();
        sql.append(STATIC_FORMATTER.execute("""
                CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionTable} (
                    id BIGSERIAL PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT,
                    stack_trace TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE,
                    originated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (message_id, originated_at) REFERENCES ${schema}.${queueTable}(id, originated_at)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS ${subscriptionTable}_message_id_idx ON ${schema}.${subscriptionTable}(message_id);
                CREATE INDEX IF NOT EXISTS ${subscriptionTable}_created_at_idx ON ${schema}.${subscriptionTable}(created_at);
                CREATE INDEX IF NOT EXISTS ${subscriptionTable}_execute_after_idx ON ${schema}.${subscriptionTable}(execute_after ASC);
                """, params));

        if (serializedByKey) {
            sql.append('\n').append(STATIC_FORMATTER.execute("""
                    ALTER TABLE ${schema}.${subscriptionTable} ADD COLUMN IF NOT EXISTS key TEXT;

                    CREATE INDEX IF NOT EXISTS ${subscriptionKeyIndex} ON ${schema}.${subscriptionTable}(key);

                    UPDATE ${schema}.${subscriptionTable} s
                    SET key = q.key
                    FROM ${schema}.${queueTable} q
                    WHERE s.key IS NULL
                        AND s.message_id = q.id
                        AND s.originated_at = q.originated_at;

                    CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionKeyLockTable} (
                        key TEXT PRIMARY KEY
                    );

                    INSERT INTO ${schema}.${subscriptionKeyLockTable}(key)
                    SELECT DISTINCT key
                    FROM ${schema}.${subscriptionTable}
                    WHERE key IS NOT NULL
                    ON CONFLICT (key) DO NOTHING;
                    """, params));
        }

        if (historyEnabled) {
            sql.append('\n').append(STATIC_FORMATTER.execute("""
                    CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionHistoryTable} (
                        id BIGINT,
                        message_id BIGINT NOT NULL,
                        attempt INTEGER NOT NULL DEFAULT 0,
                        status TEXT NOT NULL DEFAULT 'PROCESSED',
                        error_message TEXT,
                        stack_trace TEXT,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        originated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        FOREIGN KEY (message_id, originated_at) REFERENCES ${schema}.${queueTable}(id, originated_at),
                        PRIMARY KEY (id, originated_at)
                    ) PARTITION BY RANGE (originated_at);

                    CREATE UNIQUE INDEX IF NOT EXISTS ${subscriptionHistoryTable}_message_id_idx ON ${schema}.${subscriptionHistoryTable}(originated_at, message_id);
                    CREATE INDEX IF NOT EXISTS ${subscriptionHistoryTable}_created_at_idx ON ${schema}.${subscriptionHistoryTable}(created_at);
                    """, params));
        }

        sql.append('\n').append(STATIC_FORMATTER.execute("""
                CREATE OR REPLACE FUNCTION ${schema}.${insertFunction}
                  RETURNS trigger AS
                $$
                    BEGIN
                    """, params));
        sql.append('\n').append(STATIC_FORMATTER.execute(serializedByKey
                ? """
                    INSERT INTO ${schema}.${subscriptionTable}(message_id, key, created_at, execute_after, originated_at)
                         VALUES(NEW.id, NEW.key, NEW.created_at, NEW.execute_after, NEW.originated_at);

                    INSERT INTO ${schema}.${subscriptionKeyLockTable}(key)
                         VALUES(NEW.key)
                    ON CONFLICT (key) DO NOTHING;
                    """
                : """
                    INSERT INTO ${schema}.${subscriptionTable}(message_id, created_at, execute_after, originated_at)
                         VALUES(NEW.id, NEW.created_at, NEW.execute_after, NEW.originated_at);
                    """,
                params));
        sql.append('\n').append("""
                    RETURN NEW;
                    END;
                $$
                LANGUAGE 'plpgsql';
                """);
        sql.append('\n').append(STATIC_FORMATTER.execute("""
                CREATE OR REPLACE TRIGGER ${insertTrigger}
                    AFTER INSERT ON ${schema}.${queueTable}
                    FOR EACH ROW
                    EXECUTE PROCEDURE ${schema}.${insertFunction};
                """, params));

        return sql.toString();
    }
}
