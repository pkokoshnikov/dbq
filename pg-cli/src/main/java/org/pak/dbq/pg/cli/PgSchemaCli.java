package org.pak.dbq.pg.cli;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.pg.PgSchemaSqlGenerator;
import org.pak.dbq.pg.SchemaName;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(
        name = "dbq-schema",
        description = "Generate PostgreSQL DDL for DBQ queue and subscription tables.",
        mixinStandardHelpOptions = true,
        subcommands = {
                PgSchemaCli.QueueCommand.class,
                PgSchemaCli.SubscriptionCommand.class,
                PgSchemaCli.AllCommand.class
        }
)
public final class PgSchemaCli implements Runnable {
    public static void main(String[] args) {
        var exitCode = new CommandLine(new PgSchemaCli()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    private static PgSchemaSqlGenerator schemaSqlGenerator(String schema) {
        return new PgSchemaSqlGenerator(new SchemaName(schema));
    }

    private static QueueName queueName(String queue) {
        return new QueueName(queue);
    }

    private static SubscriptionId subscriptionId(String subscription) {
        return new SubscriptionId(subscription);
    }

    private abstract static class BaseQueueCommand implements Runnable {
        @Spec
        CommandSpec spec;

        @Option(names = "--schema", required = true, description = "Database schema name, for example: public")
        String schema;

        @Option(names = "--queue", required = true, description = "Logical queue name, for example: orders")
        String queue;

        final void print(String sql) {
            var out = spec.commandLine().getOut();
            out.print(sql);
            out.flush();
        }

        final PgSchemaSqlGenerator schemaSqlGenerator() {
            return PgSchemaCli.schemaSqlGenerator(schema);
        }

        final QueueName queueName() {
            return PgSchemaCli.queueName(queue);
        }
    }

    private abstract static class BaseSubscriptionCommand extends BaseQueueCommand {
        @Option(names = "--subscription", required = true, description = "Subscription id, for example: billing")
        String subscription;

        @Option(
                names = "--history-enabled",
                description = "Create history table for the subscription. Default: ${DEFAULT-VALUE}",
                defaultValue = "false",
                fallbackValue = "true",
                arity = "0..1"
        )
        boolean historyEnabled;

        final SubscriptionId subscriptionId() {
            return PgSchemaCli.subscriptionId(subscription);
        }
    }

    @Command(name = "queue", description = "Generate DDL for queue parent table.")
    static final class QueueCommand extends BaseQueueCommand {
        @Override
        public void run() {
            print(schemaSqlGenerator().createQueueTable(queueName()));
        }
    }

    @Command(name = "subscription", description = "Generate DDL for subscription live table and optional history table.")
    static final class SubscriptionCommand extends BaseSubscriptionCommand {
        @Override
        public void run() {
            print(schemaSqlGenerator().createSubscriptionTable(queueName(), subscriptionId(), historyEnabled));
        }
    }

    @Command(name = "all", description = "Generate DDL for queue table and subscription table(s).")
    static final class AllCommand extends BaseSubscriptionCommand {
        @Override
        public void run() {
            var sqlGenerator = schemaSqlGenerator();
            print(sqlGenerator.createQueueTable(queueName()));
            spec.commandLine().getOut().println();
            print(sqlGenerator.createSubscriptionTable(queueName(), subscriptionId(), historyEnabled));
        }
    }
}
