package org.pak.dbq.spi;

import java.sql.ResultSet;

@FunctionalInterface
public interface ResultSetMapper<R> {
    R map(ResultSet resultSet) throws Exception;
}
