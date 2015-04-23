package org.elasticsearch.river.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.river.mongodb.MongoConfig.Shard;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class MongoConfigProvider extends MongoDBRiverComponent implements Callable<MongoConfig> {

    private final MongoClientService mongoClientService;
    private final MongoDBRiverDefinition definition;
    private final MongoClient clusterClient;

    public MongoConfigProvider(MongoDBRiver river, MongoClientService mongoClientService) {
        super(river);
        this.mongoClientService = mongoClientService;
        this.definition = river.definition;
        this.clusterClient = mongoClientService.getMongoClusterClient(definition);
    }

    @Override
    public MongoConfig call() {
        boolean isMongos = isMongos();
        List<Shard> shards = getShards(isMongos);
        MongoConfig config = new MongoConfig(isMongos, shards);
        return config;
    }

    protected boolean ensureIsReplicaSet(MongoClient shardClient) {
        Set<String> collections = shardClient.getDB(MongoDBRiver.MONGODB_LOCAL_DATABASE).getCollectionNames();
        if (!collections.contains(MongoDBRiver.OPLOG_COLLECTION)) {
            throw new IllegalStateException("Cannot find " + MongoDBRiver.OPLOG_COLLECTION + " collection. Please check this link: http://docs.mongodb.org/manual/tutorial/deploy-replica-set/");
        }
        return true;
    }


    private DB getAdminDb() {
        DB adminDb = clusterClient.getDB(MongoDBRiver.MONGODB_ADMIN_DATABASE);
        if (adminDb == null) {
            throw new ElasticsearchException(
                    String.format("Could not get %s database from MongoDB", MongoDBRiver.MONGODB_ADMIN_DATABASE));
        }
        return adminDb;
    }

    private DB getConfigDb() {
        DB configDb = clusterClient.getDB(MongoDBRiver.MONGODB_CONFIG_DATABASE);
        if (configDb == null) {
            throw new ElasticsearchException(
                    String.format("Could not get %s database from MongoDB", MongoDBRiver.MONGODB_CONFIG_DATABASE));
        }
        return configDb;
    }

    private boolean isMongos() {
        if (definition.isMongos() != null) {
            return definition.isMongos().booleanValue();
        } else {
            DB adminDb = getAdminDb();
            if (adminDb == null) {
                return false;
            }
            logger.trace("Found {} database", MongoDBRiver.MONGODB_ADMIN_DATABASE);
            DBObject command = BasicDBObjectBuilder.start(
                    ImmutableMap.builder().put("serverStatus", 1).put("asserts", 0).put("backgroundFlushing", 0).put("connections", 0)
                            .put("cursors", 0).put("dur", 0).put("extra_info", 0).put("globalLock", 0).put("indexCounters", 0)
                            .put("locks", 0).put("metrics", 0).put("network", 0).put("opcounters", 0).put("opcountersRepl", 0)
                            .put("recordStats", 0).put("repl", 0).build()).get();
            logger.trace("About to execute: {}", command);
            CommandResult cr = adminDb.command(command, ReadPreference.primary());
            logger.trace("Command executed return : {}", cr);

            logger.info("MongoDB version - {}", cr.get("version"));
            if (logger.isTraceEnabled()) {
                logger.trace("serverStatus: {}", cr);
            }

            if (!cr.ok()) {
                logger.warn("serverStatus returns error: {}", cr.getErrorMessage());
                return false;
            }

            if (cr.get("process") == null) {
                logger.warn("serverStatus.process return null.");
                return false;
            }
            String process = cr.get("process").toString().toLowerCase();
            if (logger.isTraceEnabled()) {
                logger.trace("process: {}", process);
            }
            // Fix for https://jira.mongodb.org/browse/SERVER-9160
            return (process.contains("mongos"));
        }
    }

    private List<Shard> getShards(boolean isMongos) {
        List<Shard> shards = new ArrayList<>();
        if (isMongos) {
            try (DBCursor cursor = getConfigDb().getCollection("shards").find()) {
                while (cursor.hasNext()) {
                    DBObject item = cursor.next();
                    List<ServerAddress> shardServers = getServerAddressForReplica(item);
                    if (shardServers != null) {
                        String shardName = item.get(MongoDBRiver.MONGODB_ID_FIELD).toString();
                        MongoClient shardClient = mongoClientService.getMongoShardClient(definition, shardServers);
                        ensureIsReplicaSet(shardClient);
                        Timestamp<?> latestOplogTimestamp = getCurrentOplogTimestamp(shardClient);
                        shards.add(new Shard(shardName, shardServers, latestOplogTimestamp));
                    }
                }
            }
            return shards;
        } else {
            ensureIsReplicaSet(clusterClient);
            List<ServerAddress> servers = clusterClient.getServerAddressList();
            Timestamp<?> latestOplogTimestamp = getCurrentOplogTimestamp(clusterClient);
            shards.add(new Shard("unsharded", servers, latestOplogTimestamp));
            return shards;
        }
    }

    private List<ServerAddress> getServerAddressForReplica(DBObject item) {
        String definition = item.get("host").toString();
        if (definition.contains("/")) {
            definition = definition.substring(definition.indexOf("/") + 1);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("getServerAddressForReplica - definition: {}", definition);
        }
        List<ServerAddress> servers = new ArrayList<ServerAddress>();
        for (String server : definition.split(",")) {
            try {
                servers.add(new ServerAddress(server));
            } catch (UnknownHostException uhEx) {
                logger.warn("failed to execute bulk", uhEx);
            }
        }
        return servers;
    }
    
    private Timestamp<?> getCurrentOplogTimestamp(MongoClient shardClient) {
        DBCollection oplogCollection = shardClient
                .getDB(MongoDBRiver.MONGODB_LOCAL_DATABASE)
                .getCollection(MongoDBRiver.OPLOG_COLLECTION);
        try (DBCursor cursor = oplogCollection.find().sort(new BasicDBObject("$natural", -1)).limit(1)) {
            return Timestamp.on(cursor.next());
        }
    }

}