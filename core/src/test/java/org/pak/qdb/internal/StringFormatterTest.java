package org.pak.qdb.internal;

import org.junit.jupiter.api.Test;
import org.pak.qdb.support.StringFormatter;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StringFormatterTest {
    StringFormatter replacer = new StringFormatter();

    @Test
    void testSuccess() {
        var message = "First ${name1}! Second ${name1}! Third ${name2}!";
        var args = Map.of("name1", "test1", "name2", "test2");

        assertThat(replacer.execute(message, args)).isEqualTo("First test1! Second test1! Third test2!");
    }

    @Test
    void testFailure() {
        var message = "First ${name1}! Second ${name1}! Third ${name2}!";
        var args = Map.of("name1", "test1");
        var exception = assertThrows(IllegalArgumentException.class, () -> replacer.execute(message, args));

        assertThat(exception.getMessage()).isEqualTo("No value found for key name2");
    }

    @Test
    void testFailure2() {
        var message = "First ${name1}! Second ${name1}! Third ${name2!";
        var args = Map.of("name1", "test1");
        var exception = assertThrows(IllegalArgumentException.class, () -> replacer.execute(message, args));

        assertThat(exception.getMessage()).isEqualTo("No matching suffix for prefix at index 39");
    }

    @Test
    void testFailure3() {
        var message = "First ${${name1}}! Second ${name1}! Third ${name2!}";
        var args = Map.of("name1", "test1");
        var exception = assertThrows(IllegalArgumentException.class, () -> replacer.execute(message, args));

        assertThat(exception.getMessage()).isEqualTo("No value found for key ${name1");
    }

    @Test
    void testFailure4() {
        var message = "First ${${name1}}! Second ${name1}! Third ${name2!}";
        var args = Map.of("name1", "test1");
        var exception = assertThrows(IllegalArgumentException.class, () -> replacer.execute(message, args));

        assertThat(exception.getMessage()).isEqualTo("No value found for key ${name1");
    }
}
