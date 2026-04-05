package org.pak.dbq.spi;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;

public interface PersistenceService {
    void execute(String sql, Object... args);
    int update(String query, Object... args);
    int insert(String query, Object... args);
    int[] batchInsert(String query, List<Object[]> args);
    <R> List<R> query(String query, Function<ResultSet, R> resultSetMapper);
}
