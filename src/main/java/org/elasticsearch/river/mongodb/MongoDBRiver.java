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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
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
import org.elasticsearch.river.mongodb.MongoConfig.Shard;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.script.ScriptService;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
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

    public static final String TYPE = "mongodb";
    public static final String NAME = "mongodb-river";
    public static final String STATUS_ID = "_riverstatus";
    public static final String STATUS_FIELD = "status";
    public static final String DESCRIPTION = "MongoDB River Plugin";
    public static final String LAST_TIMESTAMP_FIELD = "_last_ts";
    public static final String LAST_GTID_FIELD = "_last_gtid";
    public static final String MONGODB_LOCAL_DATABASE = "local";
    public static final String MONGODB_ADMIN_DATABASE = "admin";
    public static final String MONGODB_CONFIG_DATABASE = "config";
    public static final String MONGODB_ID_FIELD = "_id";
    public static final String MONGODB_OID_FIELD = "oid";
    public static final String MONGODB_SEQ_FIELD = "seq";
    public static final String MONGODB_IN_OPERATOR = "$in";
    public static final String MONGODB_OR_OPERATOR = "$or";
    public static final String MONGODB_AND_OPERATOR = "$and";
    public static final String MONGODB_NATURAL_OPERATOR = "$natural";
    public static final String OPLOG_COLLECTION = "oplog.rs";
    public static final String OPLOG_REFS_COLLECTION = "oplog.refs";
    public static final String OPLOG_NAMESPACE = "ns";
    public static final String OPLOG_NAMESPACE_COMMAND = "$cmd";
    public static final String OPLOG_ADMIN_COMMAND = "admin." + OPLOG_NAMESPACE_COMMAND;
    public static final String OPLOG_OBJECT = "o";
    public static final String OPLOG_UPDATE = "o2";
    public static final String OPLOG_OPERATION = "op";
    public static final String OPLOG_UPDATE_OPERATION = "u";
    public static final String OPLOG_UPDATE_ROW_OPERATION = "ur";
    public static final String OPLOG_INSERT_OPERATION = "i";
    public static final String OPLOG_DELETE_OPERATION = "d";
    public static final String OPLOG_COMMAND_OPERATION = "c";
    public static final String OPLOG_NOOP_OPERATION = "n";
    public static final String OPLOG_DROP_COMMAND_OPERATION = "drop";
    public static final String OPLOG_DROP_DATABASE_COMMAND_OPERATION = "dropDatabase";
    public static final String OPLOG_RENAME_COLLECTION_COMMAND_OPERATION = "renameCollection";
    public static final String OPLOG_TO = "to";
    public static final String OPLOG_TIMESTAMP = "ts";
    public static final String OPLOG_FROM_MIGRATE = "fromMigrate";
    public static final String OPLOG_OPS = "ops";
    public static final String OPLOG_CREATE_COMMAND = "create";
    public static final String OPLOG_REF = "ref";
    public static final String GRIDFS_FILES_SUFFIX = ".files";
    public static final String GRIDFS_CHUNKS_SUFFIX = ".chunks";
    public static final String INSERTION_ORDER_KEY = "$natural";

    static final int MONGODB_RETRY_ERROR_DELAY_MS = 10_000;
    static final ESLogger logger = ESLoggerFactory.getLogger(MongoDBRiver.class.getName());

    protected final MongoDBRiverDefinition definition;
    protected final Client esClient;
    protected final ScriptService scriptService;
    protected final SharedContext context;

    protected volatile List<Thread> tailerThreads = Lists.newArrayList();
    protected volatile Thread indexerThread;
    protected volatile Thread statusThread;

    private final MongoClientService mongoClientService;

    @Inject
    public MongoDBRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName,
            Client esClient, ScriptService scriptService, MongoClientService mongoClientService) {
        super(riverName, settings);
        if (logger.isTraceEnabled()) {
            logger.trace("Initializing river : [{}]", riverName.getName());
        }
        this.esClient = esClient;
        this.scriptService = scriptService;
        this.mongoClientService = mongoClientService;
        this.definition = MongoDBRiverDefinition.parseSettings(riverName.name(), riverIndexName, settings, scriptService);

        BlockingQueue<QueueEntry> stream = definition.getThrottleSize() == -1 ? new LinkedTransferQueue<QueueEntry>()
                : new ArrayBlockingQueue<QueueEntry>(definition.getThrottleSize());

        this.context = new SharedContext(stream, Status.STOPPED);
    }

    @Override
    public void start() {
        try {
            logger.info("Starting river {}", riverName.getName());
            Status status = MongoDBRiverHelper.getRiverStatus(esClient, riverName.getName());
            if (status == Status.IMPORT_FAILED || status == Status.INITIAL_IMPORT_FAILED || status == Status.SCRIPT_IMPORT_FAILED
                    || status == Status.START_FAILED) {
                logger.error("Cannot start river {}. Current status is {}", riverName.getName(), status);
                return;
            }
            if (status == Status.STOPPED) {
                logger.info("Cannot start river {}. It is currently disabled", riverName.getName());
                return;
            }

            // http://stackoverflow.com/questions/5270611/read-maven-properties-file-inside-jar-war-file
            logger.info("{} - {}", DESCRIPTION, MongoDBHelper.getRiverVersion());
            logger.info(
                    "starting mongodb stream. options: secondaryreadpreference [{}], drop_collection [{}], include_collection [{}], throttlesize [{}], gridfs [{}], filter [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]",
                    definition.isMongoSecondaryReadPreference(), definition.isDropCollection(), definition.getIncludeCollection(),
                    definition.getThrottleSize(), definition.isMongoGridFS(), definition.getMongoOplogFilter(), definition.getMongoDb(),
                    definition.getMongoCollection(), definition.getScript(), definition.getIndexName(), definition.getTypeName());


            MongoDBRiverHelper.setRiverStatus(esClient, riverName.getName(), Status.RUNNING);
            this.context.setStatus(Status.RUNNING);
            for (ServerAddress server : definition.getMongoServers()) {
                logger.debug("Using mongodb server(s): host [{}], port [{}]", server.getHost(), server.getPort());
            }
            statusThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_status:" + definition.getIndexName()).newThread(
                    new StatusChecker(this, definition, context));
            statusThread.start();

            // Create the index if it does not exist
            try {
                if (!esClient.admin().indices().prepareExists(definition.getIndexName()).get().isExists()) {
                    esClient.admin().indices().prepareCreate(definition.getIndexName()).get();
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
                    esClient.admin().indices().preparePutMapping(definition.getIndexName()).setType(definition.getTypeName())
                            .setSource(getGridFSMapping()).get();
                } catch (Exception e) {
                    logger.warn("Failed to set explicit mapping (attachment): {}", e);
                }
            }

            // Replicate data roughly the same way MongoDB does
            // https://groups.google.com/d/msg/mongodb-user/sOKlhD_E2ns/SvngoUHXtcAJ
            //
            // Steps:
            // Get oplog timestamp
            // Do the initial import
            // Sync from the oplog of each shard starting at timestamp
            //
            // Notes
            // Primary difference between river sync and MongoDB replica sync is that we ignore chunk migrations
            // We only need to know about CRUD commands. If data moves from one MongoDB shard to another
            // then we do not need to let ElasticSearch know that.
            MongoClient mongoClusterClient = mongoClientService.getMongoClusterClient(definition);
            MongoConfigProvider configProvider = new MongoConfigProvider(mongoClientService, definition);
            MongoConfig config;
            while (true) {
                try {
                    config = configProvider.call();
                    break;
                } catch(MongoSocketException | MongoTimeoutException e) {
                    Thread.sleep(MONGODB_RETRY_ERROR_DELAY_MS);
                }
            }

            Timestamp startTimestamp = null;
            if (definition.getInitialTimestamp() != null) {
                startTimestamp = definition.getInitialTimestamp();
            } else if (getLastProcessedTimestamp() != null) {
                startTimestamp = getLastProcessedTimestamp();
            } else {
                for (Shard shard : config.getShards()) {
                    if (startTimestamp == null || shard.getLatestOplogTimestamp().compareTo(startTimestamp) < 1) {
                        startTimestamp = shard.getLatestOplogTimestamp();
                    }
                }
            }

            indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_indexer:" + definition.getIndexName()).newThread(
                    new Indexer(this, definition, context, esClient, scriptService));
            indexerThread.start();

            // Import in main thread to block tailing the oplog            
            CollectionSlurper importer = new CollectionSlurper(startTimestamp, mongoClusterClient, definition, context, esClient);
            importer.run();

            // Tail the oplog
            if (config.isMongos()) {
                for (Shard shard : config.getShards()) {
                    MongoClient mongoClient = mongoClientService.getMongoShardClient(definition, shard.getReplicas());
                    Thread tailerThread = EsExecutors.daemonThreadFactory(
                            settings.globalSettings(), "mongodb_river_slurper_" + shard.getName() + ":" + definition.getIndexName()
                        ).newThread(new OplogSlurper(shard.getLatestOplogTimestamp(), mongoClusterClient, mongoClient, definition, context, esClient));
                    tailerThreads.add(tailerThread);                   
                }
            } else {
                Shard shard = config.getShards().get(0);
                Thread tailerThread = EsExecutors.daemonThreadFactory(
                        settings.globalSettings(), "mongodb_river_slurper_" + shard.getName() + ":" + definition.getIndexName()
                    ).newThread(new OplogSlurper(shard.getLatestOplogTimestamp(), mongoClusterClient, mongoClusterClient, definition, context, esClient));
                tailerThreads.add(tailerThread);                   
            }

            for (Thread thread : tailerThreads) {
                thread.start();
            }
        } catch (Throwable t) {
            logger.warn("Failed to start river {}", t, riverName.getName());
            MongoDBRiverHelper.setRiverStatus(esClient, definition.getRiverName(), Status.START_FAILED);
            this.context.setStatus(Status.START_FAILED);
        }
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

    protected Timestamp<?> getLastProcessedTimestamp() {
      return MongoDBRiver.getLastTimestamp(esClient, definition);
    }

    private XContentBuilder getGridFSMapping() throws IOException {
        XContentBuilder mapping = jsonBuilder()
            .startObject()
                .startObject(definition.getTypeName())
                    .startObject("properties")
                        .startObject("content").field("type", "attachment").endObject()
                        .startObject("filename").field("type", "string").endObject()
                        .startObject("contentType").field("type", "string").endObject()
                        .startObject("md5").field("type", "string").endObject()
                        .startObject("length").field("type", "long").endObject()
                        .startObject("chunkSize").field("type", "long").endObject()
                    .endObject()
               .endObject()
           .endObject();
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
