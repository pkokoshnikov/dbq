package org.pak.dbq.pg.cli;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.pg.PgSchemaSqlGenerator;

public final class PgSchemaCli {
    private PgSchemaCli() {
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            fail("Usage: queue <schema> <queue-name> | subscription <schema> <queue-name> <subscription-name> [history-enabled] | all <schema> <queue-name> <subscription-name> [history-enabled]");
            return;
        }

        try {
            var command = args[0];
            var schemaName = new SchemaName(args[1]);
            var queueName = new QueueName(args[2]);
            var sqlGenerator = new PgSchemaSqlGenerator(schemaName);

            switch (command) {
                case "queue" -> System.out.print(sqlGenerator.createQueueTable(queueName));
                case "subscription" -> {
                    requireArgs(args, 4);
                    System.out.print(sqlGenerator.createSubscriptionTable(
                            queueName,
                            new SubscriptionId(args[3]),
                            parseHistoryEnabled(args, 4)));
                }
                case "all" -> {
                    requireArgs(args, 4);
                    var subscriptionId = new SubscriptionId(args[3]);
                    System.out.print(sqlGenerator.createQueueTable(queueName));
                    System.out.println();
                    System.out.print(sqlGenerator.createSubscriptionTable(
                            queueName,
                            subscriptionId,
                            parseHistoryEnabled(args, 4)));
                }
                default -> fail("Unknown command: " + command);
            }
        } catch (IllegalArgumentException e) {
            fail(e.getMessage());
        }
    }

    private static void requireArgs(String[] args, int required) {
        if (args.length < required) {
            throw new IllegalArgumentException("Not enough arguments");
        }
    }

    private static boolean parseHistoryEnabled(String[] args, int index) {
        return args.length > index && Boolean.parseBoolean(args[index]);
    }

    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }
}
