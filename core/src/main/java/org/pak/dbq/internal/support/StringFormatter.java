package org.pak.dbq.internal.support;

import java.util.Map;

public class StringFormatter {
    private String prefix = "${";
    private String suffix = "}";

    public StringFormatter(String prefix, String suffix) {
        this.prefix = prefix;
        this.suffix = suffix;
    }

    public StringFormatter() {
    }

    public String execute(String message, Map<String, String> args) {
        StringBuilder sb = new StringBuilder();

        int fromIndex = 0;
        do {
            var prefixIndex = message.indexOf(prefix, fromIndex);
            if (prefixIndex == -1) {
                sb.append(message, fromIndex, message.length());
                break;
            } else {
                sb.append(message, fromIndex, prefixIndex);
                var suffixIndex = message.indexOf(suffix, prefixIndex);
                if (suffixIndex == -1) {
                    throw new IllegalArgumentException("No matching suffix for prefix at index " + prefixIndex);
                } else {
                    var key = message.substring(prefixIndex + prefix.length(), suffixIndex);
                    var value = args.get(key);
                    if (value == null) {
                        throw new IllegalArgumentException("No value found for key " + key);
                    } else {
                        sb.append(value);
                    }
                    fromIndex = suffixIndex + suffix.length();
                }
            }
        } while (true);

        return sb.toString();
    }

}
