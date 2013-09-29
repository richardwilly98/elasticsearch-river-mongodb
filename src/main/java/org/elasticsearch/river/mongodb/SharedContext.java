package org.elasticsearch.river.mongodb;

import java.util.concurrent.BlockingQueue;

import org.elasticsearch.river.mongodb.MongoDBRiver.QueueEntry;

/**
 * Holds mutable state to be shared between river, slurper, and indexer.
 */
public class SharedContext {

    private BlockingQueue<QueueEntry> stream;
    private boolean active;

    public SharedContext(BlockingQueue<QueueEntry> stream, boolean active) {
        this.stream = stream;
        this.active = active;
    }

    public BlockingQueue<QueueEntry> getStream() {
        return stream;
    }

    public void setStream(BlockingQueue<QueueEntry> stream) {
        this.stream = stream;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

}
