/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.mongodb;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.bson.types.BSONTimestamp;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.script.ScriptService;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.util.JSON;

/**
 * @author richardwilly98 (Richard Louapre)
 * @author flaper87 (Flavio Percoco Premoli)
 * @author aparo (Alberto Paro)
 * @author kryptt (Rodolfo Hansen)
 * @author benmccann (Ben McCann)
 */
public class MongoDBRiver extends AbstractRiverComponent implements River {

    public final static String TYPE = "mongodb";
    public final static String NAME = "mongodb-river";
    public final static String STATUS_ID = "_riverstatus";
    public final static String STATUS_FIELD = "status";
    public final static String DESCRIPTION = "MongoDB River Plugin";
    public final static String LAST_TIMESTAMP_FIELD = "_last_ts";
    public final static String MONGODB_LOCAL_DATABASE = "local";
    public final static String MONGODB_ADMIN_DATABASE = "admin";
    public final static String MONGODB_CONFIG_DATABASE = "config";
    public final static String MONGODB_ID_FIELD = "_id";
    public final static String MONGODB_IN_OPERATOR = "$in";
    public final static String MONGODB_OR_OPERATOR = "$or";
    public final static String MONGODB_AND_OPERATOR = "$and";
    public final static String MONGODB_NATURAL_OPERATOR = "$natural";
    public final static String OPLOG_COLLECTION = "oplog.rs";
    public final static String OPLOG_NAMESPACE = "ns";
    public final static String OPLOG_NAMESPACE_COMMAND = "$cmd";
    public final static String OPLOG_OBJECT = "o";
    public final static String OPLOG_UPDATE = "o2";
    public final static String OPLOG_OPERATION = "op";
    public final static String OPLOG_UPDATE_OPERATION = "u";
    public final static String OPLOG_INSERT_OPERATION = "i";
    public final static String OPLOG_DELETE_OPERATION = "d";
    public final static String OPLOG_COMMAND_OPERATION = "c";
    public final static String OPLOG_DROP_COMMAND_OPERATION = "drop";
    public final static String OPLOG_DROP_DATABASE_COMMAND_OPERATION = "dropDatabase";
    public final static String OPLOG_TIMESTAMP = "ts";
    public final static String OPLOG_FROM_MIGRATE = "fromMigrate";
    public final static String GRIDFS_FILES_SUFFIX = ".files";
    public final static String GRIDFS_CHUNKS_SUFFIX = ".chunks";

    static final ESLogger logger = ESLoggerFactory.getLogger(MongoDBRiver.class.getName());

    protected final MongoDBRiverDefinition definition;
    protected final Client client;
    protected final ScriptService scriptService;
    protected final SharedContext context;

    protected volatile List<Thread> tailerThreads = new ArrayList<Thread>();
    protected volatile Thread indexerThread;
    protected volatile Thread statusThread;
    protected volatile boolean startInvoked = false;

    private Mongo mongo;
    private DB adminDb;

    @Inject
    public MongoDBRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client,
            ScriptService scriptService) {
        super(riverName, settings);
        if (logger.isTraceEnabled()) {
            logger.trace("Initializing river : [{}]", riverName.getName());
        }
        this.scriptService = scriptService;
        this.client = client;
        this.definition = MongoDBRiverDefinition.parseSettings(riverName.name(), riverIndexName, settings, scriptService);

        BlockingQueue<QueueEntry> stream = definition.getThrottleSize() == -1 ? new LinkedTransferQueue<QueueEntry>()
                : new ArrayBlockingQueue<QueueEntry>(definition.getThrottleSize());

        this.context = new SharedContext(stream, Status.STOPPED);
    }

    @Override
    public void start() {
        try {
            logger.info("Starting river {}", riverName.getName());
            if (MongoDBRiverHelper.getRiverStatus(client, riverName.getName()) == Status.STOPPED) {
                logger.debug("Cannot start river {}. It is currently disabled", riverName.getName());
                startInvoked = true;
                return;
            }

            MongoDBRiverHelper.setRiverStatus(client, riverName.getName(), Status.RUNNING);
            this.context.setStatus(Status.RUNNING);
            for (ServerAddress server : definition.getMongoServers()) {
                logger.debug("Using mongodb server(s): host [{}], port [{}]", server.getHost(), server.getPort());
            }
            // http://stackoverflow.com/questions/5270611/read-maven-properties-file-inside-jar-war-file
            logger.info("{} version: [{}]", DESCRIPTION, MongoDBHelper.getRiverVersion());
            logger.info(
                    "starting mongodb stream. options: secondaryreadpreference [{}], drop_collection [{}], include_collection [{}], throttlesize [{}], gridfs [{}], filter [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]",
                    definition.isMongoSecondaryReadPreference(), definition.isDropCollection(), definition.getIncludeCollection(),
                    definition.getThrottleSize(), definition.isMongoGridFS(), definition.getMongoOplogFilter(), definition.getMongoDb(),
                    definition.getMongoCollection(), definition.getScript(), definition.getIndexName(), definition.getTypeName());

            // Create the index if it does not exist
            try {
                if (!client.admin().indices().prepareExists(definition.getIndexName()).execute().actionGet().isExists()) {
                    client.admin().indices().prepareCreate(definition.getIndexName()).execute().actionGet();
                }
            } catch (Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                    // that's fine
                } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                    // ok, not recovered yet..., lets start indexing and hope we
                    // recover by the first bulk
                    // TODO: a smarter logic can be to register for cluster
                    // event
                    // listener here, and only start sampling when the
                    // block is removed...
                } else {
                    logger.warn("failed to create index [{}], disabling river...", e, definition.getIndexName());
                    return;
                }
            }

            // GridFS
            if (definition.isMongoGridFS()) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Set explicit attachment mapping.");
                    }
                    client.admin().indices().preparePutMapping(definition.getIndexName()).setType(definition.getTypeName())
                            .setSource(getGridFSMapping()).execute().actionGet();
                } catch (Exception e) {
                    logger.warn("Failed to set explicit mapping (attachment): {}", e);
                }
            }

            // Tail the oplog
            if (isMongos()) {
                DBCursor cursor = getConfigDb().getCollection("shards").find();
                try {
                    while (cursor.hasNext()) {
                        DBObject item = cursor.next();
                        logger.debug("shards: {}", item.toString());
                        List<ServerAddress> servers = getServerAddressForReplica(item);
                        if (servers != null) {
                            String replicaName = item.get(MONGODB_ID_FIELD).toString();
                            Thread tailerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                                    "mongodb_river_slurper_" + replicaName).newThread(new Slurper(servers, definition, context, client));
                            tailerThreads.add(tailerThread);
                        }
                    }
                } finally {
                    cursor.close();
                }
            } else {
                Thread tailerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_slurper").newThread(
                        new Slurper(definition.getMongoServers(), definition, context, client));
                tailerThreads.add(tailerThread);
            }

            for (Thread thread : tailerThreads) {
                thread.start();
            }

            indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_indexer").newThread(
                    new Indexer(definition, context, client, scriptService));
            indexerThread.start();

            statusThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_status").newThread(
                    new StatusChecker(this, definition, context));
            statusThread.start();
        } catch (Throwable t) {
            logger.warn("Fail to start river {}", t, riverName.getName());
            MongoDBRiverHelper.setRiverStatus(client, definition.getRiverName(), Status.START_FAILED);
            this.context.setStatus(Status.START_FAILED);
        } finally {
            startInvoked = true;
        }
    }

    private boolean isMongos() {
        DB adminDb = getAdminDb();
        if (adminDb == null) {
            return false;
        }
        CommandResult cr = adminDb.command(new BasicDBObject("serverStatus", 1));

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

    private DB getAdminDb() {
        if (adminDb == null) {
            adminDb = getMongoClient().getDB(MONGODB_ADMIN_DATABASE);
            logger.info("MongoAdminUser: {} - isAuthenticated: {}", definition.getMongoAdminUser(), adminDb.isAuthenticated());
            if (!definition.getMongoAdminUser().isEmpty() && !definition.getMongoAdminPassword().isEmpty() && !adminDb.isAuthenticated()) {
                logger.info("Authenticate {} with {}", MONGODB_ADMIN_DATABASE, definition.getMongoAdminUser());

                try {
                    CommandResult cmd = adminDb.authenticateCommand(definition.getMongoAdminUser(), definition.getMongoAdminPassword()
                            .toCharArray());
                    if (!cmd.ok()) {
                        logger.error("Autenticatication failed for {}: {}", MONGODB_ADMIN_DATABASE, cmd.getErrorMessage());
                    }
                } catch (MongoException mEx) {
                    logger.warn("getAdminDb() failed", mEx);
                }
            }
        }
        return adminDb;
    }

    private DB getConfigDb() {
        DB configDb = getMongoClient().getDB(MONGODB_CONFIG_DATABASE);
        if (!definition.getMongoAdminUser().isEmpty() && !definition.getMongoAdminPassword().isEmpty() && getAdminDb().isAuthenticated()) {
            configDb = getAdminDb().getMongo().getDB(MONGODB_CONFIG_DATABASE);
            // } else if (!mongoDbUser.isEmpty() && !mongoDbPassword.isEmpty()
            // && !configDb.isAuthenticated()) {
            // logger.info("Authenticate {} with {}", mongoDb, mongoDbUser);
            // CommandResult cmd = configDb.authenticateCommand(mongoDbUser,
            // mongoDbPassword.toCharArray());
            // if (!cmd.ok()) {
            // logger.error("Authentication failed for {}: {}",
            // DB_CONFIG, cmd.getErrorMessage());
            // }
        }
        return configDb;
    }

    private Mongo getMongoClient() {
        if (mongo == null) {
            mongo = new MongoClient(definition.getMongoServers(), definition.getMongoClientOptions());
        }
        return mongo;
    }

    private void closeMongoClient() {
        logger.info("Closing Mongo client");
        if (adminDb != null) {
            adminDb = null;
        }
        if (mongo != null) {
            mongo.close();
            mongo = null;
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

    @Override
    public void close() {
        logger.info("Closing river {}", riverName.getName());
        try {
            statusThread.interrupt();
            statusThread = null;
            for (Thread thread : tailerThreads) {
                thread.interrupt();
                thread = null;
            }
            tailerThreads.clear();
            if (indexerThread != null) {
                indexerThread.interrupt();
                indexerThread = null;
            }
            closeMongoClient();
        } catch (Throwable t) {
            logger.error("Fail to close river {}", t, riverName.getName());
        } finally {
            this.context.setStatus(Status.STOPPED);
        }
    }

    private XContentBuilder getGridFSMapping() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject(definition.getTypeName()).startObject("properties")
                .startObject("content").field("type", "attachment").endObject().startObject("filename").field("type", "string").endObject()
                .startObject("contentType").field("type", "string").endObject().startObject("md5").field("type", "string").endObject()
                .startObject("length").field("type", "long").endObject().startObject("chunkSize").field("type", "long").endObject()
                .endObject().endObject().endObject();
        logger.info("Mapping: {}", mapping.string());
        return mapping;
    }

    /**
     * Get the latest timestamp for a given namespace.
     */
    @SuppressWarnings("unchecked")
    public static BSONTimestamp getLastTimestamp(Client client, MongoDBRiverDefinition definition) {

        GetResponse lastTimestampResponse = client
                .prepareGet(definition.getRiverIndexName(), definition.getRiverName(), definition.getMongoOplogNamespace()).execute()
                .actionGet();

        // API changes since 0.90.0 lastTimestampResponse.exists() replaced by
        // lastTimestampResponse.isExists()
        if (lastTimestampResponse.isExists()) {
            // API changes since 0.90.0 lastTimestampResponse.sourceAsMap()
            // replaced by lastTimestampResponse.getSourceAsMap()
            Map<String, Object> mongodbState = (Map<String, Object>) lastTimestampResponse.getSourceAsMap().get(TYPE);
            if (mongodbState != null) {
                String lastTimestamp = mongodbState.get(LAST_TIMESTAMP_FIELD).toString();
                if (lastTimestamp != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} last timestamp: {}", definition.getMongoOplogNamespace(), lastTimestamp);
                    }
                    return (BSONTimestamp) JSON.parse(lastTimestamp);

                }
            }
        } else {
            if (definition.getInitialTimestamp() != null) {
                return definition.getInitialTimestamp();
            }
        }
        return null;
    }

    /**
     * Adds an index request operation to a bulk request, updating the last
     * timestamp for a given namespace (ie: host:dbName.collectionName)
     * 
     * @param bulk
     */
    static void updateLastTimestamp(final MongoDBRiverDefinition definition, final BSONTimestamp time, final BulkProcessor bulkProcessor) {
        try {
            bulkProcessor.add(indexRequest(definition.getRiverIndexName())
                    .type(definition.getRiverName())
                    .id(definition.getMongoOplogNamespace())
                    .source(jsonBuilder().startObject().startObject(TYPE).field(LAST_TIMESTAMP_FIELD, JSON.serialize(time)).endObject()
                            .endObject()));
        } catch (IOException e) {
            logger.error("error updating last timestamp for namespace {}", definition.getMongoOplogNamespace());
        }
    }

    public static long getIndexCount(Client client, MongoDBRiverDefinition definition) {
        if (client.admin().indices().prepareTypesExists(definition.getIndexName()).setTypes(definition.getTypeName()).get().isExists()) {
            CountResponse countResponse = client.prepareCount(definition.getIndexName()).setTypes(definition.getTypeName()).execute()
                    .actionGet();
            return countResponse.getCount();
        } else {
            return 0;
        }
    }

    protected static class QueueEntry {

        private final DBObject data;
        private final String operation;
        private final BSONTimestamp oplogTimestamp;

        public QueueEntry(DBObject data) {
            this(null, OPLOG_INSERT_OPERATION, data);
        }

        public QueueEntry(BSONTimestamp oplogTimestamp, String oplogOperation, DBObject data) {
            this.data = data;
            this.operation = oplogOperation;
            this.oplogTimestamp = oplogTimestamp;
        }

        public boolean isOplogEntry() {
            return oplogTimestamp != null;
        }

        public boolean isAttachment() {
            return (data instanceof GridFSDBFile);
        }

        public DBObject getData() {
            return data;
        }

        public String getOperation() {
            return operation;
        }

        public BSONTimestamp getOplogTimestamp() {
            return oplogTimestamp;
        }

    }

}
