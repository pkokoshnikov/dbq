package org.pak.dbq.pg;

import org.junit.jupiter.api.Test;
import org.pak.dbq.spi.PersistenceService;
import org.pak.dbq.spi.ResultSetMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueueServiceTest {

    @Test
    void createPartitionUsesUtcBoundariesForRange() throws Exception {
        var persistenceService = new CapturingPersistenceService();
        var partitionService = new QueuePartitionService(new SchemaName("public"), persistenceService);

        partitionService.createPartition("test_message", Instant.parse("2026-04-03T23:30:00Z"));

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
        public <R> List<R> query(String query, ResultSetMapper<R> resultSetMapper) {
            throw new UnsupportedOperationException();
        }
    }
}
