package org.pak.dbq.pg;

public enum DropPartitionResult {
    DROPPED,
    ALREADY_ABSENT,
    HAS_REFERENCES
}
