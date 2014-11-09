package org.elasticsearch.river.mongodb;

import java.util.concurrent.BlockingQueue;

import org.elasticsearch.river.mongodb.MongoDBRiver.QueueEntry;

/**
 * Holds mutable state to be shared between river, slurper, and indexer.
 */
public class SharedContext {

    private final BlockingQueue<QueueEntry> stream;
    private Status status;

    public SharedContext(BlockingQueue<QueueEntry> stream, Status status) {
        this.stream = stream;
        this.status = status;
    }

    public BlockingQueue<QueueEntry> getStream() {
        return stream;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

}
