package org.pak.dbq.spi;

import org.pak.dbq.error.DbqException;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;

public interface PersistenceService {
    void execute(String sql, Object... args) throws DbqException;
    int update(String query, Object... args) throws DbqException;
    int insert(String query, Object... args) throws DbqException;
    int[] batchInsert(String query, List<Object[]> args) throws DbqException;
    <R> List<R> query(String query, Function<ResultSet, R> resultSetMapper) throws DbqException;
}
