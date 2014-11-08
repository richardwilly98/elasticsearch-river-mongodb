package org.elasticsearch.river.mongodb;

import java.util.List;

import com.mongodb.ServerAddress;

public class MongoConfig {

    private final boolean isMongos;
    private final List<Shard> shards;
    
    public MongoConfig(boolean isMongos, List<Shard> shards) {
        this.isMongos = isMongos;
        this.shards = shards;
    }

    public boolean isMongos() {
        return isMongos;
    }

    public List<Shard> getShards() {
        return shards;
    }

    public static class Shard {
        
        private final String name;
        private final List<ServerAddress> replicas;

        public Shard(String name, List<ServerAddress> replicas) {
            this.name = name;
            this.replicas = replicas;
        }
        public String getName() {
            return name;
        }
        public List<ServerAddress> getReplicas() {
            return replicas;
        }        
    }

}
