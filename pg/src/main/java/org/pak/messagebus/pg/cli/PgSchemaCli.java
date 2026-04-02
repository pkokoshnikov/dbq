package org.pak.messagebus.pg.cli;

import org.pak.messagebus.core.MessageName;
import org.pak.messagebus.core.SchemaName;
import org.pak.messagebus.core.SubscriptionName;
import org.pak.messagebus.pg.PgSchemaSqlGenerator;

public final class PgSchemaCli {
    private PgSchemaCli() {
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            fail("Usage: message <schema> <message-name> | subscription <schema> <message-name> <subscription-name> | all <schema> <message-name> <subscription-name>");
            return;
        }

        try {
            var command = args[0];
            var schemaName = new SchemaName(args[1]);
            var messageName = new MessageName(args[2]);
            var sqlGenerator = new PgSchemaSqlGenerator(schemaName);

            switch (command) {
                case "message" -> System.out.print(sqlGenerator.createMessageTable(messageName));
                case "subscription" -> {
                    requireArgs(args, 4);
                    System.out.print(sqlGenerator.createSubscriptionTable(messageName, new SubscriptionName(args[3])));
                }
                case "all" -> {
                    requireArgs(args, 4);
                    var subscriptionName = new SubscriptionName(args[3]);
                    System.out.print(sqlGenerator.createMessageTable(messageName));
                    System.out.println();
                    System.out.print(sqlGenerator.createSubscriptionTable(messageName, subscriptionName));
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
