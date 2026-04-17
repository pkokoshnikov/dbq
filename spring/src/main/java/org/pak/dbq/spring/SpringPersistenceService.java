package org.pak.dbq.spring;

import org.pak.dbq.spi.error.NonRetrayablePersistenceException;
import org.pak.dbq.spi.error.PersistenceException;
import org.pak.dbq.spi.error.RetryablePersistenceException;
import org.pak.dbq.spi.PersistenceService;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.dao.TransientDataAccessException;

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
    public void execute(String query, Object... args) throws PersistenceException {
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

    public int update(String query, Object... args) throws PersistenceException {
        try {
            return jdbcTemplate.update(query, args);
        } catch (Exception e) {
            classifyException(e);
            return 0;
        }
    }

    public int insert(String query, Object... args) throws PersistenceException {
        try {
            return jdbcTemplate.update(query, args);
        } catch (Exception e) {
            classifyException(e);
            return 0;
        }
    }

    @Override
    public int[] batchInsert(String query, List<Object[]> args) throws PersistenceException {
        try {
            return jdbcTemplate.batchUpdate(query, args);
        } catch (Exception e) {
            classifyException(e);
            return new int[0];
        }
    }

    private void classifyException(Exception e) throws PersistenceException {
        if (e instanceof TransientDataAccessException
                || e instanceof RecoverableDataAccessException
                || e instanceof CannotGetJdbcConnectionException) {
            throw new RetryablePersistenceException(e, e.getCause());
        }
        throw new NonRetrayablePersistenceException(e, e.getCause());
    }

    @Override
    public <R> List<R> query(String query, Function<ResultSet, R> mapper) throws PersistenceException {
        try {
            return jdbcTemplate.query(query, (rs, rowNum) -> mapper.apply(rs));
        } catch (Exception e) {
            classifyException(e);
            return null;
        }
    }
}
