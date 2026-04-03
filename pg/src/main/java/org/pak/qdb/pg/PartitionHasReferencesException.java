package org.pak.qdb.pg;

import org.pak.qdb.api.error.PersistenceException;

public class PartitionHasReferencesException extends RuntimeException {

    public PartitionHasReferencesException(PersistenceException e) {
        super(e);
    }
}
