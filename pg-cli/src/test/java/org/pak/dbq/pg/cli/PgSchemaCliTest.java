package org.pak.dbq.pg.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;

class PgSchemaCliTest {
    @Test
    void queueCommandRendersQueueSql() {
        var out = new StringWriter();
        var err = new StringWriter();
        var commandLine = new CommandLine(new PgSchemaCli())
                .setOut(new PrintWriter(out))
                .setErr(new PrintWriter(err));

        var exitCode = commandLine.execute("queue", "--schema", "public", "--queue", "orders");

        assertThat(exitCode).isZero();
        assertThat(out.toString()).contains("CREATE TABLE IF NOT EXISTS public.orders");
        assertThat(err.toString()).isEmpty();
    }

    @Test
    void subscriptionCommandSupportsHistoryFlag() {
        var out = new StringWriter();
        var err = new StringWriter();
        var commandLine = new CommandLine(new PgSchemaCli())
                .setOut(new PrintWriter(out))
                .setErr(new PrintWriter(err));

        var exitCode = commandLine.execute(
                "subscription",
                "--schema", "public",
                "--queue", "orders",
                "--subscription", "billing",
                "--history-enabled"
        );

        assertThat(exitCode).isZero();
        assertThat(out.toString()).contains("billing_history");
        assertThat(err.toString()).isEmpty();
    }

    @Test
    void showsUsageForMissingRequiredOptions() {
        var out = new StringWriter();
        var err = new StringWriter();
        var commandLine = new CommandLine(new PgSchemaCli())
                .setOut(new PrintWriter(out))
                .setErr(new PrintWriter(err));

        var exitCode = commandLine.execute("queue");

        assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
        assertThat(err.toString()).contains("--schema").contains("--queue");
    }
}
