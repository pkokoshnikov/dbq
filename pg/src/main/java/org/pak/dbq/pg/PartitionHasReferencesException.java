package org.pak.dbq.pg;

import org.pak.dbq.spi.error.PersistenceException;

public class PartitionHasReferencesException extends RuntimeException {

    public PartitionHasReferencesException(PersistenceException e) {
        super(e);
    }
}
