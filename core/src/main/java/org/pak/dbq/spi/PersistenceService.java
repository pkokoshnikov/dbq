package org.pak.dbq.spi;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;

import org.pak.dbq.spi.error.PersistenceException;

public interface PersistenceService {
    void execute(String sql, Object... args) throws PersistenceException;
    int update(String query, Object... args) throws PersistenceException;
    int insert(String query, Object... args) throws PersistenceException;
    int[] batchInsert(String query, List<Object[]> args) throws PersistenceException;
    <R> List<R> query(String query, Function<ResultSet, R> resultSetMapper) throws PersistenceException;
}
