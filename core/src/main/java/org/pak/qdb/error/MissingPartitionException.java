package org.pak.qdb.error;

import lombok.Getter;

import java.time.Instant;
import java.util.List;

public class MissingPartitionException extends RuntimeException {
    @Getter
    private final List<Instant> originationTimes;

    public MissingPartitionException(List<Instant> originationTimes) {
        this.originationTimes = originationTimes;
    }
}
