package org.pak.dbq.pg.jsonb;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.error.MessageSerializationException;
import org.pak.dbq.spi.error.NonRetrayablePersistenceException;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JsonbConverter {
    private final ObjectMapper objectMapper;
    private final Map<Class, String> classTypeMap = new HashMap<>();
    private final Map<String, Class> typeClassMap = new HashMap<>();

    public JsonbConverter() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        objectMapper.coercionConfigDefaults().setCoercion(CoercionInputShape.EmptyString, CoercionAction.AsNull);
    }

    public JsonbConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void registerType(String name, Class clazz) {
        classTypeMap.put(clazz, name);
        typeClassMap.put(name, clazz);
        objectMapper.addMixIn(clazz, JsonbMixin.class);
    }

    public <T> T toJsonb(PGobject source) {
        try {
            JsonNode tree = objectMapper.readTree(source.getValue());
            if (tree.get("@type") != null) {
                var aClass = typeClassMap.get(tree.get("@type").asText());

                if (aClass == null) {
                    log.warn("Type {} is not found", tree.get("@type").asText());
                    return null;
                }

                return (T) objectMapper.treeToValue(tree, aClass);
            } else {
                log.warn("@type field is not found");
                return null;
            }
        } catch (JsonProcessingException e) {
            throw new MessageSerializationException(e);
        }
    }

    public <T extends PGobject> T toPGObject(Object source) {
        try {
            var value = objectMapper.writerFor(source.getClass())
                    .withAttribute("@type", classTypeMap.get(source.getClass()))
                    .writeValueAsString(source);

            var jsonObject = new PGobject();
            jsonObject.setType("jsonb");
            jsonObject.setValue(value);

            return (T) jsonObject;
        } catch (SQLException e) {
            throw new NonRetrayablePersistenceException(e, e.getCause());
        } catch (JsonProcessingException e) {
            throw new MessageSerializationException(e);
        }
    }

    public Map<String, String> toStringMap(PGobject source) {
        try {
            if (source == null || source.getValue() == null) {
                return Map.of();
            }

            return objectMapper.readValue(
                    source.getValue(),
                    objectMapper.getTypeFactory().constructMapType(HashMap.class, String.class, String.class)
            );
        } catch (JsonProcessingException e) {
            throw new MessageSerializationException(e);
        }
    }

    public <T extends PGobject> T toPGObject(Map<String, String> source) {
        try {
            var jsonObject = new PGobject();
            jsonObject.setType("jsonb");
            jsonObject.setValue(objectMapper.writeValueAsString(
                    source == null ? Collections.<String, String>emptyMap() : source));

            return (T) jsonObject;
        } catch (SQLException e) {
            throw new NonRetrayablePersistenceException(e, e.getCause());
        } catch (JsonProcessingException e) {
            throw new MessageSerializationException(e);
        }
    }
}
