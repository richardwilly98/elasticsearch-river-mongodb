package org.elasticsearch.river.mongodb;

public enum Status {

    UNKNOWN,
    /** River is starting up */
    STARTING,
    START_FAILED,
    RUNNING,
    STOPPED,
    IMPORT_FAILED,
    INITIAL_IMPORT_FAILED,
    SCRIPT_IMPORT_FAILED,
    RIVER_STALE;

}
