package org.pak.qdb.spring;

import org.pak.qdb.api.error.NonRetrayablePersistenceException;
import org.pak.qdb.api.error.RetrayablePersistenceException;
import org.pak.qdb.spi.PersistenceService;
import org.springframework.jdbc.BadSqlGrammarException;
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
    public void execute(String query, Object... args) {
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

    public int update(String query, Object... args) {
        try {
            return jdbcTemplate.update(query, args);
        } catch (Exception e) {
            classifyException(e);
            return 0;
        }
    }

    public int insert(String query, Object... args) {
        try {
            return jdbcTemplate.update(query, args);
        } catch (Exception e) {
            classifyException(e);
            return 0;
        }
    }

    @Override
    public int[] batchInsert(String query, List<Object[]> args) {
        try {
            return jdbcTemplate.batchUpdate(query, args);
        } catch (Exception e) {
            classifyException(e);
            return new int[0];
        }
    }

    private void classifyException(Exception e) {
        if (BadSqlGrammarException.class.isAssignableFrom(e.getClass())) {
            throw new NonRetrayablePersistenceException(e, e.getCause());
        } else {
            throw new RetrayablePersistenceException(e, e.getCause());
        }
    }

    @Override
    public <R> List<R> query(String query, Function<ResultSet, R> mapper) {
        try {
            return jdbcTemplate.query(query, (rs, rowNum) -> mapper.apply(rs));
        } catch (Exception e) {
            classifyException(e);
            return null;
        }
    }
}
