package org.pak.messagebus.pg;

import org.pak.messagebus.core.MessageName;
import org.pak.messagebus.core.SchemaName;
import org.pak.messagebus.core.StringFormatter;
import org.pak.messagebus.core.SubscriptionName;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class PgSchemaSqlGenerator {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy_MM_dd");

    private final SchemaName schemaName;
    private final StringFormatter formatter = new StringFormatter();

    public PgSchemaSqlGenerator(SchemaName schemaName) {
        this.schemaName = schemaName;
    }

    public String createMessageTable(MessageName messageName) {
        return formatter.execute("""
                CREATE TABLE IF NOT EXISTS ${schema}.${messageTable} (
                    id BIGSERIAL,
                    key TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    originated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    payload JSONB NOT NULL,
                    PRIMARY KEY (id, originated_at)
                ) PARTITION BY RANGE (originated_at);
                CREATE INDEX IF NOT EXISTS ${messageTable}_created_at_idx ON ${schema}.${messageTable}(created_at);
                CREATE UNIQUE INDEX IF NOT EXISTS ${messageTable}_message_key_idx ON ${schema}.${messageTable}(originated_at, key);
                """, Map.of("schema", schemaName.value(), "messageTable", messageTable(messageName)));
    }

    public String createSubscriptionTable(MessageName messageName, SubscriptionName subscriptionName) {
        return formatter.execute("""
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
                            FOREIGN KEY (message_id, originated_at) REFERENCES ${schema}.${messageTable}(id, originated_at)
                        );

                        CREATE UNIQUE INDEX IF NOT EXISTS ${subscriptionTable}_message_id_idx ON ${schema}.${subscriptionTable}(message_id);
                        CREATE INDEX IF NOT EXISTS ${subscriptionTable}_created_at_idx ON ${schema}.${subscriptionTable}(created_at);
                        CREATE INDEX IF NOT EXISTS ${subscriptionTable}_execute_after_idx ON ${schema}.${subscriptionTable}(execute_after ASC);

                        CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionHistoryTable} (
                            id BIGINT,
                            message_id BIGINT NOT NULL,
                            attempt INTEGER NOT NULL DEFAULT 0,
                            status TEXT NOT NULL DEFAULT 'PROCESSED',
                            error_message TEXT,
                            stack_trace TEXT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            originated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            FOREIGN KEY (message_id, originated_at) REFERENCES ${schema}.${messageTable}(id, originated_at),
                            PRIMARY KEY (id, originated_at)
                        ) PARTITION BY RANGE (originated_at);

                        CREATE UNIQUE INDEX IF NOT EXISTS ${subscriptionHistoryTable}_message_id_idx ON ${schema}.${subscriptionHistoryTable}(originated_at, message_id);
                        CREATE INDEX IF NOT EXISTS ${subscriptionHistoryTable}_created_at_idx ON ${schema}.${subscriptionHistoryTable}(created_at);

                        CREATE OR REPLACE FUNCTION ${schema}.${insertFunction}
                          RETURNS trigger AS
                        $$
                            BEGIN
                            INSERT INTO ${schema}.${subscriptionTable}(message_id, created_at, execute_after, originated_at)
                                 VALUES(NEW.id, NEW.created_at, NEW.execute_after, NEW.originated_at);
                            RETURN NEW;
                            END;
                        $$
                        LANGUAGE 'plpgsql';

                        CREATE OR REPLACE TRIGGER ${insertTrigger}
                            AFTER INSERT ON ${schema}.${messageTable}
                            FOR EACH ROW
                            EXECUTE PROCEDURE ${schema}.${insertFunction};
                        """,
                Map.of("schema", schemaName.value(), "messageTable", messageTable(messageName), "subscriptionTable",
                        subscriptionTable(subscriptionName), "subscriptionHistoryTable",
                        subscriptionHistoryTable(subscriptionName), "insertTrigger",
                        subscriptionTable(subscriptionName) + "_insert_trigger", "insertFunction",
                        subscriptionTable(subscriptionName) + "_insert_function()"));
    }

    public String createPartition(String table, Instant dateTime) {
        var date = dateTime.atOffset(ZoneOffset.UTC).toLocalDate();
        var partition = table + "_" + DATE_FORMATTER.format(date);

        return formatter.execute("""
                CREATE TABLE IF NOT EXISTS ${schema}.${partition}
                PARTITION OF ${schema}.${table} FOR VALUES FROM ('${from}') TO ('${to}');
                """, Map.of(
                "schema", schemaName.value(),
                "table", table,
                "partition", partition,
                "from", DATE_FORMATTER.format(date),
                "to", DATE_FORMATTER.format(date.plus(1, ChronoUnit.DAYS))
        ));
    }

    public String dropPartition(String table, String partition) {
        return formatter.execute("""
                ALTER TABLE ${schema}.${table} DETACH PARTITION ${schema}.${partition} CONCURRENTLY;
                DROP TABLE IF EXISTS ${schema}.${partition};
                """, Map.of("schema", schemaName.value(), "table", table, "partition", partition));
    }

    public String partitionName(String table, LocalDate partition) {
        return table + "_" + DATE_FORMATTER.format(partition);
    }

    public String messageTable(MessageName messageName) {
        return messageName.name().replace("-", "_");
    }

    public String subscriptionTable(SubscriptionName subscriptionName) {
        return subscriptionName.name().replace("-", "_");
    }

    public String subscriptionHistoryTable(SubscriptionName subscriptionName) {
        return subscriptionName.name().replace("-", "_") + "_history";
    }
}
