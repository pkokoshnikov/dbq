package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.error.NonRetryablePersistenceException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.pg.jsonb.JsonbConverter;
import org.postgresql.util.PGobject;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;

import static java.util.Optional.ofNullable;

public final class MessageContainerMapper {
    private final JsonbConverter jsonbConverter;

    public MessageContainerMapper(JsonbConverter jsonbConverter) {
        this.jsonbConverter = jsonbConverter;
    }

    @SuppressWarnings("unchecked")
    public <T> MessageContainer<T> map(ResultSet rs) {
        try {
            return new MessageContainer<>(rs.getObject("id", BigInteger.class),
                    rs.getObject("message_id", BigInteger.class),
                    rs.getString("key"),
                    rs.getInt("attempt"),
                    ofNullable(rs.getObject("execute_after", OffsetDateTime.class)).map(OffsetDateTime::toInstant)
                            .orElse(null),
                    ofNullable(rs.getObject("created_at", OffsetDateTime.class)).map(OffsetDateTime::toInstant)
                            .orElse(null),
                    ofNullable(rs.getObject("updated_at", OffsetDateTime.class)).map(OffsetDateTime::toInstant)
                            .orElse(null),
                    ofNullable(rs.getObject("originated_at", OffsetDateTime.class)).map(OffsetDateTime::toInstant)
                            .orElse(null),
                    jsonbConverter.fromPGobject(rs.getObject("payload", PGobject.class)),
                    jsonbConverter.fromPGHeaders(rs.getObject("headers", PGobject.class)),
                    rs.getString("error_message"),
                    rs.getString("stack_trace"));
        } catch (SQLException e) {
            return sneakyThrow(new NonRetryablePersistenceException(e, e.getCause()));
        } catch (DbqException e) {
            return sneakyThrow(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T, E extends Throwable> T sneakyThrow(Throwable throwable) throws E {
        throw (E) throwable;
    }
}
