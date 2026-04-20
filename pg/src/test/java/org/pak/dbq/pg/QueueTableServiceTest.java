package org.pak.dbq.pg;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;

import static org.assertj.core.api.Assertions.assertThat;

class QueueTableServiceTest {
    private static final SchemaName SCHEMA_NAME = new SchemaName("public");
    private static final QueueName QUEUE_NAME = new QueueName("orders");
    private static final SubscriptionId SUBSCRIPTION_ID = new SubscriptionId("billing");

    @Test
    void createQueueTableSqlContainsQueueSchemaStatements() {
        var sql = QueueTableService.createQueueTableSql(SCHEMA_NAME, QUEUE_NAME);

        assertThat(sql).contains("CREATE TABLE IF NOT EXISTS public.orders");
        assertThat(sql).contains("PRIMARY KEY (id, originated_at)");
        assertThat(sql).contains("PARTITION BY RANGE (originated_at);");
        assertThat(sql).contains("CREATE INDEX IF NOT EXISTS orders_created_at_idx ON public.orders(created_at);");
        assertThat(sql).contains("CREATE UNIQUE INDEX IF NOT EXISTS orders_message_key_idx ON public.orders(originated_at, key);");
    }

    @Test
    void createSubscriptionTableSqlWithoutSerializedByKeyDoesNotContainKeyLockStatements() {
        var sql = QueueTableService.createSubscriptionTableSql(
                SCHEMA_NAME,
                QUEUE_NAME,
                SUBSCRIPTION_ID,
                false,
                false);

        assertThat(sql).contains("CREATE OR REPLACE FUNCTION public.billing_insert_function()");
        assertThat(sql).contains("CREATE OR REPLACE TRIGGER billing_insert_trigger");
        assertThat(sql).contains("INSERT INTO public.billing(message_id, created_at, execute_after, originated_at)");
        assertThat(sql).contains("VALUES(NEW.id, NEW.created_at, NEW.execute_after, NEW.originated_at);");
        assertThat(sql).doesNotContain("public.billing_key_lock");
        assertThat(sql).doesNotContain("ADD COLUMN IF NOT EXISTS key TEXT");
        assertThat(sql).doesNotContain("INSERT INTO public.billing(message_id, key, created_at, execute_after, originated_at)");
    }

    @Test
    void createSubscriptionTableSqlWithSerializedByKeyContainsKeyLockStatements() {
        var sql = QueueTableService.createSubscriptionTableSql(
                SCHEMA_NAME,
                QUEUE_NAME,
                SUBSCRIPTION_ID,
                false,
                true);

        assertThat(sql).contains("ALTER TABLE public.billing ADD COLUMN IF NOT EXISTS key TEXT;");
        assertThat(sql).contains("CREATE TABLE IF NOT EXISTS public.billing_key_lock");
        assertThat(sql).contains("INSERT INTO public.billing(message_id, key, created_at, execute_after, originated_at)");
        assertThat(sql).contains("VALUES(NEW.id, NEW.key, NEW.created_at, NEW.execute_after, NEW.originated_at);");
        assertThat(sql).contains("INSERT INTO public.billing_key_lock(key)");
        assertThat(sql).contains("VALUES(NEW.key)");
        assertThat(sql).contains("ON CONFLICT (key) DO NOTHING;");
        assertThat(sql).contains("CREATE OR REPLACE FUNCTION public.billing_insert_function()");
        assertThat(sql).contains("CREATE OR REPLACE TRIGGER billing_insert_trigger");
    }
}
