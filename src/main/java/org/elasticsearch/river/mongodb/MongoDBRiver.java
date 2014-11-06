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
import java.util.concurrent.LinkedTransferQueue;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
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

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFSDBFile;

/**
 * @author richardwilly98 (Richard Louapre)
 * @author flaper87 (Flavio Percoco Premoli)
 * @author aparo (Alberto Paro)
 * @author kryptt (Rodolfo Hansen)
 * @author benmccann (Ben McCann)
 * @author kdkeck (Kevin Keck)
 */
public class MongoDBRiver extends AbstractRiverComponent implements River {

    public final static String TYPE = "mongodb";
    public final static String NAME = "mongodb-river";
    public final static String STATUS_ID = "_riverstatus";
    public final static String STATUS_FIELD = "status";
    public final static String DESCRIPTION = "MongoDB River Plugin";
    public final static String LAST_TIMESTAMP_FIELD = "_last_ts";
    public final static String LAST_GTID_FIELD = "_last_gtid";
    public final static String MONGODB_LOCAL_DATABASE = "local";
    public final static String MONGODB_ADMIN_DATABASE = "admin";
    public final static String MONGODB_CONFIG_DATABASE = "config";
    public final static String MONGODB_ID_FIELD = "_id";
    public final static String MONGODB_OID_FIELD = "oid";
    public final static String MONGODB_SEQ_FIELD = "seq";
    public final static String MONGODB_IN_OPERATOR = "$in";
    public final static String MONGODB_OR_OPERATOR = "$or";
    public final static String MONGODB_AND_OPERATOR = "$and";
    public final static String MONGODB_NATURAL_OPERATOR = "$natural";
    public final static String OPLOG_COLLECTION = "oplog.rs";
    public final static String OPLOG_REFS_COLLECTION = "oplog.refs";
    public final static String OPLOG_NAMESPACE = "ns";
    public final static String OPLOG_NAMESPACE_COMMAND = "$cmd";
    public final static String OPLOG_ADMIN_COMMAND = "admin." + OPLOG_NAMESPACE_COMMAND;
    public final static String OPLOG_OBJECT = "o";
    public final static String OPLOG_UPDATE = "o2";
    public final static String OPLOG_OPERATION = "op";
    public final static String OPLOG_UPDATE_OPERATION = "u";
    public final static String OPLOG_UPDATE_ROW_OPERATION = "ur";
    public final static String OPLOG_INSERT_OPERATION = "i";
    public final static String OPLOG_DELETE_OPERATION = "d";
    public final static String OPLOG_COMMAND_OPERATION = "c";
    public final static String OPLOG_NOOP_OPERATION = "n";
    public final static String OPLOG_DROP_COMMAND_OPERATION = "drop";
    public final static String OPLOG_DROP_DATABASE_COMMAND_OPERATION = "dropDatabase";
    public final static String OPLOG_RENAME_COLLECTION_COMMAND_OPERATION = "renameCollection";
    public final static String OPLOG_TO = "to";
    public final static String OPLOG_TIMESTAMP = "ts";
    public final static String OPLOG_FROM_MIGRATE = "fromMigrate";
    public static final String OPLOG_OPS = "ops";
    public static final String OPLOG_CREATE_COMMAND = "create";
    public static final String OPLOG_REF = "ref";
    public final static String GRIDFS_FILES_SUFFIX = ".files";
    public final static String GRIDFS_CHUNKS_SUFFIX = ".chunks";
    public final static String INSERTION_ORDER_KEY = "$natural";

    static final ESLogger logger = ESLoggerFactory.getLogger(MongoDBRiver.class.getName());

    protected final MongoDBRiverDefinition definition;
    protected final Client client;
    protected final ScriptService scriptService;
    protected final SharedContext context;

    protected volatile List<Thread> tailerThreads = Lists.newArrayList();
    protected volatile Thread indexerThread;
    protected volatile Thread statusThread;
    protected volatile boolean startInvoked = false;

    private final MongoClientService mongoClientService;
    private DB adminDb;

    @Inject
    public MongoDBRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client,
            ScriptService scriptService,
            MongoClientService mongoClientService) {
        super(riverName, settings);
        if (logger.isTraceEnabled()) {
            logger.trace("Initializing river : [{}]", riverName.getName());
        }
        this.scriptService = scriptService;
        this.mongoClientService = mongoClientService;
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
            Status status = MongoDBRiverHelper.getRiverStatus(client, riverName.getName());
            if (status == Status.IMPORT_FAILED || status == Status.INITIAL_IMPORT_FAILED || status == Status.SCRIPT_IMPORT_FAILED
                    || status == Status.START_FAILED) {
                logger.error("Cannot start river {}. Current status is {}", riverName.getName(), status);
                return;
            }
            if (status == Status.STOPPED) {
                logger.info("Cannot start river {}. It is currently disabled", riverName.getName());
                startInvoked = true;
                return;
            }

            MongoDBRiverHelper.setRiverStatus(client, riverName.getName(), Status.RUNNING);
            this.context.setStatus(Status.RUNNING);
            for (ServerAddress server : definition.getMongoServers()) {
                logger.debug("Using mongodb server(s): host [{}], port [{}]", server.getHost(), server.getPort());
            }
            // http://stackoverflow.com/questions/5270611/read-maven-properties-file-inside-jar-war-file
            logger.info("{} - {}", DESCRIPTION, MongoDBHelper.getRiverVersion());
            logger.info(
                    "starting mongodb stream. options: secondaryreadpreference [{}], drop_collection [{}], include_collection [{}], throttlesize [{}], gridfs [{}], filter [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]",
                    definition.isMongoSecondaryReadPreference(), definition.isDropCollection(), definition.getIncludeCollection(),
                    definition.getThrottleSize(), definition.isMongoGridFS(), definition.getMongoOplogFilter(), definition.getMongoDb(),
                    definition.getMongoCollection(), definition.getScript(), definition.getIndexName(), definition.getTypeName());

            // Create the index if it does not exist
            try {
                if (!client.admin().indices().prepareExists(definition.getIndexName()).get().isExists()) {
                    client.admin().indices().prepareCreate(definition.getIndexName()).get();
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
                    logger.error("failed to create index [{}], disabling river...", e, definition.getIndexName());
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
                            .setSource(getGridFSMapping()).get();
                } catch (Exception e) {
                    logger.warn("Failed to set explicit mapping (attachment): {}", e);
                }
            }

            // Tail the oplog
            if (isMongos()) {
                try (DBCursor cursor = getConfigDb().getCollection("shards").find()) {
                    while (cursor.hasNext()) {
                        DBObject item = cursor.next();
                        logger.debug("shards: {}", item.toString());
                        List<ServerAddress> servers = getServerAddressForReplica(item);
                        if (servers != null) {
                            String replicaName = item.get(MONGODB_ID_FIELD).toString();
                            Thread tailerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                                    "mongodb_river_slurper_" + replicaName + ":" + definition.getIndexName()).newThread(new Slurper(getReplicaMongoClient(servers), definition, context, client));
                            tailerThreads.add(tailerThread);
                        }
                    }
                }
            } else {
                logger.trace("Not mongos");
                Thread tailerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_slurper:" + definition.getIndexName()).newThread(
                        new Slurper(getMongoClient(), definition, context, client));
                tailerThreads.add(tailerThread);
            }

            for (Thread thread : tailerThreads) {
                thread.start();
            }

            indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_indexer:" + definition.getIndexName()).newThread(
                    new Indexer(this, definition, context, client, scriptService));
            indexerThread.start();

            statusThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_status:" + definition.getIndexName()).newThread(
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
        if (definition.isMongos() != null) {
            return definition.isMongos().booleanValue();
        } else {
            DB adminDb = getAdminDb();
            if (adminDb == null) {
                return false;
            }
            logger.trace("Found {} database", MONGODB_ADMIN_DATABASE);
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

    private DB getAdminDb() {
        if (adminDb == null) {
        	if(!definition.getMongoAdminAuthDatabase().isEmpty()) {
	        	adminDb = getMongoClient().getDB(definition.getMongoAdminAuthDatabase());
        	} else {
            	adminDb = getMongoClient().getDB(MONGODB_ADMIN_DATABASE);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("MongoAdminUser: {} - authenticated: {}", definition.getMongoAdminUser(), adminDb.isAuthenticated());
            }
            if (!definition.getMongoAdminUser().isEmpty() && !definition.getMongoAdminPassword().isEmpty() && !adminDb.isAuthenticated()) {
                logger.info("Authenticate {} with {}", MONGODB_ADMIN_DATABASE, definition.getMongoAdminUser());

                try {
                    CommandResult cmd = adminDb.authenticateCommand(definition.getMongoAdminUser(), definition.getMongoAdminPassword()
                            .toCharArray());
                    if (!cmd.ok()) {
                        logger.error("Authentication failed for {}: {}", MONGODB_ADMIN_DATABASE, cmd.getErrorMessage());
                    } else {
                        logger.trace("authenticateCommand: {} - isAuthenticated: {}", cmd, adminDb.isAuthenticated());
                    }
                } catch (MongoException mEx) {
                    logger.warn("getAdminDb() failed", mEx);
                }
            }
            adminDb = adminDb.getMongo().getDB(MONGODB_ADMIN_DATABASE);
        }
        if (adminDb == null) {
            throw new ElasticsearchException(String.format("Could not get %s database from MongoDB", MONGODB_ADMIN_DATABASE));
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
        if (configDb == null) {
            throw new ElasticsearchException(String.format("Could not get %s database from MongoDB", MONGODB_CONFIG_DATABASE));
        }
        return configDb;
    }

    private Mongo getMongoClient() {
        return mongoClientService.getMongoClient(definition.getMongoServers(), definition.getMongoClientOptions());
    }

    private Mongo getReplicaMongoClient(List<ServerAddress> servers) {
        return mongoClientService.getMongoClient(servers, definition.getMongoClientOptions());
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
            if (statusThread != null) {
                statusThread.interrupt();
                statusThread = null;
            }
            for (Thread thread : tailerThreads) {
                thread.interrupt();
                thread = null;
            }
            tailerThreads.clear();
            if (indexerThread != null) {
                indexerThread.interrupt();
                indexerThread = null;
            }
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
        logger.info("GridFS Mapping: {}", mapping.string());
        return mapping;
    }

    /**
     * Get the latest timestamp for a given namespace.
     */
    @SuppressWarnings("unchecked")
    public static Timestamp<?> getLastTimestamp(Client client, MongoDBRiverDefinition definition) {

        client.admin().indices().prepareRefresh(definition.getRiverIndexName()).get();

        GetResponse lastTimestampResponse = client.prepareGet(definition.getRiverIndexName(), definition.getRiverName(),
                definition.getMongoOplogNamespace()).get();

        if (lastTimestampResponse.isExists()) {
            Map<String, Object> mongodbState = (Map<String, Object>) lastTimestampResponse.getSourceAsMap().get(TYPE);
            if (mongodbState != null) {
                Timestamp<?> lastTimestamp = Timestamp.on(mongodbState);
                if (lastTimestamp != null) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("{} last timestamp: {}", definition.getMongoOplogNamespace(), lastTimestamp);
                    }
                    return lastTimestamp;
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
    static void setLastTimestamp(final MongoDBRiverDefinition definition, final Timestamp<?> time, final BulkProcessor bulkProcessor) {
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("setLastTimestamp [{}] [{}] [{}]", definition.getRiverName(), definition.getMongoOplogNamespace(), time);
            }
            bulkProcessor.add(indexRequest(definition.getRiverIndexName()).type(definition.getRiverName())
                    .id(definition.getMongoOplogNamespace()).source(source(time)));
        } catch (IOException e) {
            logger.error("error updating last timestamp for namespace {}", definition.getMongoOplogNamespace());
        }
    }

    private static XContentBuilder source(Timestamp<?> time) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().startObject(TYPE);
        time.saveFields(builder);
        return builder.endObject().endObject();
    }

    public static long getIndexCount(Client client, MongoDBRiverDefinition definition) {
        if (client.admin().indices().prepareExists(definition.getIndexName()).get().isExists()) {
            if (definition.isImportAllCollections()) {
                return client.prepareCount(definition.getIndexName()).execute().actionGet().getCount();
            } else {
                if (client.admin().indices().prepareTypesExists(definition.getIndexName()).setTypes(definition.getTypeName()).get()
                        .isExists()) {
                    return client.prepareCount(definition.getIndexName()).setTypes(definition.getTypeName()).get().getCount();
                }
            }
        }
        return 0;
    }

    protected static class QueueEntry {

        private final DBObject data;
        private final Operation operation;
        private final Timestamp<?> oplogTimestamp;
        private final String collection;

        public QueueEntry(DBObject data, String collection) {
            this(null, Operation.INSERT, data, collection);
        }

        public QueueEntry(Timestamp<?> oplogTimestamp, Operation oplogOperation, DBObject data, String collection) {
            this.data = data;
            this.operation = oplogOperation;
            this.oplogTimestamp = oplogTimestamp;
            this.collection = collection;
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

        public Operation getOperation() {
            return operation;
        }

        public Timestamp<?> getOplogTimestamp() {
            return oplogTimestamp;
        }

        public String getCollection() {
            return collection;
        }
    }

}
