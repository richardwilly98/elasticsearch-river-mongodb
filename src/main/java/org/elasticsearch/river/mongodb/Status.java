package org.elasticsearch.river.mongodb;

public enum Status {

    UNKNOWN,
    RUNNING,
    STOPPED,
    IMPORT_FAILED,
    INITIAL_IMPORT_FAILED;
    
}
