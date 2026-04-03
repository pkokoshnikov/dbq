package org.pak.qdb.pg.jsonb;

import com.fasterxml.jackson.databind.annotation.JsonAppend;

@JsonAppend(
        attrs = {
                @JsonAppend.Attr(value = "@type")
        }
)
public interface JsonbMixin {
}
