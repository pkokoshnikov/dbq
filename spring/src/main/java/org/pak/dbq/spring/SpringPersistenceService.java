package org.pak.dbq.spring;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.error.MessageDeserializationException;
import org.pak.dbq.error.MessageSerializationException;
import org.pak.dbq.spi.PersistenceService;
import org.pak.dbq.error.NonRetryablePersistenceException;
import org.pak.dbq.error.RetryablePersistenceException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;

public class SpringPersistenceService implements PersistenceService {
    private final JdbcTemplate jdbcTemplate;

    public SpringPersistenceService(
            JdbcTemplate jdbcTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void execute(String query, Object... args) throws DbqException {
        try {
            jdbcTemplate.execute(con -> con.prepareStatement(query),
                    (PreparedStatementCallback<Object>) ps -> {
                        new ArgumentPreparedStatementSetter(args).setValues(ps);
                        ps.execute();
                        return null;
                    });
        } catch (Exception e) {
            classifyException(e);
        }
    }

    public int update(String query, Object... args) throws DbqException {
        try {
            return jdbcTemplate.update(query, args);
        } catch (Exception e) {
            classifyException(e);
            return 0;
        }
    }

    public int insert(String query, Object... args) throws DbqException {
        try {
            return jdbcTemplate.update(query, args);
        } catch (Exception e) {
            classifyException(e);
            return 0;
        }
    }

    @Override
    public int[] batchInsert(String query, List<Object[]> args) throws DbqException {
        try {
            return jdbcTemplate.batchUpdate(query, args);
        } catch (Exception e) {
            classifyException(e);
            return new int[0];
        }
    }

    private void classifyException(Exception e) throws DbqException {
        if (e instanceof MessageSerializationException messageSerializationException) {
            throw messageSerializationException;
        }
        if (e instanceof MessageDeserializationException messageDeserializationException) {
            throw messageDeserializationException;
        }
        if (e instanceof TransientDataAccessException
                || e instanceof RecoverableDataAccessException
                || e instanceof CannotGetJdbcConnectionException) {
            throw new RetryablePersistenceException(e, e.getCause());
        }
        throw new NonRetryablePersistenceException(e, e.getCause());
    }

    @Override
    public <R> List<R> query(String query, Function<ResultSet, R> mapper) throws DbqException {
        try {
            return jdbcTemplate.query(query, (rs, rowNum) -> mapper.apply(rs));
        } catch (Exception e) {
            classifyException(e);
            return null;
        }
    }
}
