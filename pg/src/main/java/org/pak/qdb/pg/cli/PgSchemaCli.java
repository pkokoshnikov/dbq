package org.pak.qdb.pg.cli;

import org.pak.qdb.api.QueueName;
import org.pak.qdb.pg.SchemaName;
import org.pak.qdb.api.SubscriptionId;
import org.pak.qdb.pg.PgSchemaSqlGenerator;

public final class PgSchemaCli {
    private PgSchemaCli() {
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            fail("Usage: queue <schema> <queue-name> | subscription <schema> <queue-name> <subscription-name> | all <schema> <queue-name> <subscription-name>");
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
                    System.out.print(sqlGenerator.createSubscriptionTable(queueName, new SubscriptionId(args[3])));
                }
                case "all" -> {
                    requireArgs(args, 4);
                    var subscriptionId = new SubscriptionId(args[3]);
                    System.out.print(sqlGenerator.createQueueTable(queueName));
                    System.out.println();
                    System.out.print(sqlGenerator.createSubscriptionTable(queueName, subscriptionId));
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

    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }
}
