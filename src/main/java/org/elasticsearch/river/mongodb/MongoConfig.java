package org.elasticsearch.river.mongodb;

import java.util.List;

import com.mongodb.ServerAddress;

public class MongoConfig {

    private boolean isMongos;
    private final List<Shard> shards;
    
    public MongoConfig(boolean isMongos, List<Shard> shards) {
        this.isMongos = isMongos;
        this.shards = shards;
    }

    public List<Shard> getShards() {
        return shards;
    }

    public boolean isMongos() {
        return isMongos;
    }

    public static class Shard {
        
        private final String name;
        private final List<ServerAddress> replicas;
        private final Timestamp<?> latestOplogTimestamp;

        public Shard(String name, List<ServerAddress> replicas, Timestamp<?> latestOplogTimestamp) {
            this.name = name;
            this.replicas = replicas;
            this.latestOplogTimestamp = latestOplogTimestamp;
        }

        public String getName() {
            return name;
        }
        public List<ServerAddress> getReplicas() {
            return replicas;
        }
        public Timestamp<?> getLatestOplogTimestamp() {
            return latestOplogTimestamp;
        }
    }

}