package org.pak.messagebus.pg;

import org.junit.jupiter.api.Test;
import org.pak.messagebus.core.SchemaName;
import org.pak.messagebus.core.service.PersistenceService;
import org.pak.messagebus.pg.jsonb.JsonbConverter;

import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class PgQueryServiceTest {

    @Test
    void createPartitionUsesUtcBoundariesForRange() {
        var persistenceService = new CapturingPersistenceService();
        var queryService = new PgQueryService(persistenceService, new SchemaName("public"), new JsonbConverter());

        queryService.createPartition("test_message", Instant.parse("2026-04-03T23:30:00Z"));

        assertThat(persistenceService.executedQueries).singleElement().satisfies(query -> {
            assertThat(query).contains("FOR VALUES FROM ('2026-04-03T00:00:00Z')");
            assertThat(query).contains("TO ('2026-04-04T00:00:00Z')");
            assertThat(query).doesNotContain("FOR VALUES FROM ('2026_04_03')");
            assertThat(query).doesNotContain("TO ('2026_04_04')");
        });
    }

    private static class CapturingPersistenceService implements PersistenceService {
        private final List<String> executedQueries = new ArrayList<>();

        @Override
        public void execute(String sql, Object... args) {
            executedQueries.add(sql);
        }

        @Override
        public int update(String query, Object... args) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int insert(String query, Object... args) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] batchInsert(String query, List<Object[]> args) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <R> List<R> query(String query, Function<ResultSet, R> resultSetMapper) {
            throw new UnsupportedOperationException();
        }
    }
}
