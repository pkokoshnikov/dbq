package org.pak.qdb.pg;

import lombok.experimental.FieldDefaults;

@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
public class SchemaName {
    String name;

    public SchemaName(String name){
        if (!name.matches("^[a-z_]+$")) {
            throw new IllegalArgumentException("Schema name must be lowercase");
        }
        this.name = name;
    }

    public String value() {
        return name;
    }
}
