package org.elasticsearch.river.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.mongodb.MongoConfig.Shard;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class MongoConfigProvider implements Callable<MongoConfig> {

    private static final ESLogger logger = ESLoggerFactory.getLogger(MongoConfigProvider.class.getName());

    private final MongoDBRiverDefinition definition;
    private final MongoClient client;

    public MongoConfigProvider(MongoDBRiverDefinition definition, MongoClient client) {
        this.definition= definition;
        this.client = client;
    }

    @Override
    public MongoConfig call() {
        boolean isMongos = isMongos();
        List<Shard> shards = isMongos ? getShards() : new ArrayList<Shard>();
        MongoConfig config = new MongoConfig(isMongos, shards);
        return config;
    }

    private DB getAdminDb() {
        DB adminDb = client.getDB(MongoDBRiver.MONGODB_ADMIN_DATABASE);
        if (adminDb == null) {
            throw new ElasticsearchException(
                    String.format("Could not get %s database from MongoDB", MongoDBRiver.MONGODB_ADMIN_DATABASE));
        }
        return adminDb;
    }

    private DB getConfigDb() {
        DB configDb = client.getDB(MongoDBRiver.MONGODB_CONFIG_DATABASE);
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

    private List<Shard> getShards() {
        List<Shard> shards = new ArrayList<>();
        try (DBCursor cursor = getConfigDb().getCollection("shards").find()) {
            while (cursor.hasNext()) {
                DBObject item = cursor.next();
                List<ServerAddress> servers = getServerAddressForReplica(item);
                if (servers != null) {
                    String replicaName = item.get(MongoDBRiver.MONGODB_ID_FIELD).toString();
                    shards.add(new Shard(replicaName, servers));
                }
            }
        }
        return shards;
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
    
}
