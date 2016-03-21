package org.elasticsearch.river.mongodb;

import java.util.concurrent.atomic.AtomicLong;

import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.QueryOperators;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

class CollectionSlurper extends MongoDBRiverComponent {

    private final MongoDBRiverDefinition definition;
    private final SharedContext context;
    private final Client esClient;
    private final MongoClient mongoClient;
    private final DB slurpedDb;
    private final AtomicLong totalDocuments = new AtomicLong();

    public CollectionSlurper(MongoDBRiver river, MongoClient mongoClient) {
        super(river);
        this.definition = river.definition;
        this.context = river.context;
        this.esClient = river.esClient;
        this.mongoClient = mongoClient;
        this.slurpedDb = mongoClient.getDB(definition.getMongoDb());
    }

    /**
     * Import initial contents from the {@code definition}
     *
     * @param timestamp the timestamp to use for the last imported document
     */
    public void importInitial(Timestamp<?> timestamp) {
        try {
            if (!isIndexEmpty()) {
                // MongoDB would delete the index and re-attempt the import
                // We should probably do that too or at least have an option for it
                // https://groups.google.com/d/msg/mongodb-user/hrOuS-lpMeI/opP6l0gndSEJ
                logger.error("Cannot import collection {} into existing index", definition.getMongoCollection());
                MongoDBRiverHelper.setRiverStatus(
                        esClient, definition.getRiverName(), Status.INITIAL_IMPORT_FAILED);
                return;
            }
            if (definition.isImportAllCollections()) {
                for (String name : slurpedDb.getCollectionNames()) {
                    if (name.length() < 7 || !name.substring(0, 7).equals("system.")) {
                        DBCollection collection = slurpedDb.getCollection(name);
                        importCollection(collection, timestamp);
                    }
                }
            } else {
                DBCollection collection = slurpedDb.getCollection(definition.getMongoCollection());
                importCollection(collection, timestamp);
            }
        } catch (MongoInterruptedException | InterruptedException e) {
            logger.error("river-mongodb slurper interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Exception in initial import", e);
            logger.debug("Total documents inserted so far: {}", totalDocuments.get());
            Thread.currentThread().interrupt();
        }
    }

    protected boolean isIndexEmpty() {
        return MongoDBRiver.getIndexCount(esClient, definition) == 0;
    }

    /**
     * Import a single collection into the index
     *
     * @param collection the collection to import
     * @param timestamp the timestamp to use for the last imported document
     * @throws InterruptedException
     *             if the blocking queue stream is interrupted while waiting
     */
    public void importCollection(DBCollection collection, Timestamp<?> timestamp) throws InterruptedException {
        // TODO: ensure the index type is empty
        // DBCollection slurpedCollection =
        // slurpedDb.getCollection(definition.getMongoCollection());

        logger.info("MongoDBRiver is beginning initial import of " + collection.getFullName());
        boolean inProgress = true;
        String lastId = null;
        while (inProgress) {
            DBCursor cursor = null;
            try {
                if (definition.isDisableIndexRefresh()) {
                    updateIndexRefresh(definition.getIndexName(), -1L);
                }
                if (!definition.isMongoGridFS()) {
                    if (logger.isTraceEnabled()) {
                        // Note: collection.count() is expensive on TokuMX
                        logger.trace("Collection {} - count: {}", collection.getName(), safeCount(collection, timestamp.getClass()));
                    }
                    long count = 0;
                    cursor = collection
                            .find(getFilterForInitialImport(definition.getMongoCollectionFilter(), lastId))
                            .sort(new BasicDBObject("_id", 1));
                    while (cursor.hasNext() && context.getStatus() == Status.RUNNING) {
                        DBObject object = cursor.next();
                        count++;
                        if (cursor.hasNext()) {
                          lastId = addInsertToStream(null, applyFieldFilter(object), collection.getName());
                        } else {
                          logger.debug("Last entry for initial import of {} - add timestamp: {}", collection.getFullName(), timestamp);
                          lastId = addInsertToStream(timestamp, applyFieldFilter(object), collection.getName());
                        }
                    }
                    inProgress = false;
                    logger.info("Number of documents indexed in initial import of {}: {}", collection.getFullName(), count);
                } else {
                    // TODO: To be optimized.
                    // https://github.com/mongodb/mongo-java-driver/pull/48#issuecomment-25241988
                    // possible option: Get the object id list from .fs collection then call GriDFS.findOne
                    GridFS grid = new GridFS(mongoClient.getDB(definition.getMongoDb()), definition.getMongoCollection());

                    cursor = grid.getFileList();
                    while (cursor.hasNext()) {
                        DBObject object = cursor.next();
                        if (object instanceof GridFSDBFile) {
                            GridFSDBFile file = grid.findOne(new ObjectId(object.get(MongoDBRiver.MONGODB_ID_FIELD).toString()));
                            if (cursor.hasNext()) {
                              lastId = addInsertToStream(null, file);
                            } else {
                              logger.debug("Last entry for initial import of {} - add timestamp: {}", collection.getFullName(), timestamp);
                              lastId = addInsertToStream(timestamp, file);
                            }
                        }
                    }
                    inProgress = false;
                }
            } catch (MongoSocketException | MongoTimeoutException | MongoCursorNotFoundException e) {
                logger.info("Initial import - {} - {}. Will retry.", e.getClass().getSimpleName(), e.getMessage());
                logger.debug("Total documents inserted so far: {}", totalDocuments.get());
                Thread.sleep(MongoDBRiver.MONGODB_RETRY_ERROR_DELAY_MS);
            } finally {
                if (cursor != null) {
                    logger.trace("Closing initial import cursor");
                    cursor.close();
                }
                if (definition.isDisableIndexRefresh()) {
                    updateIndexRefresh(definition.getIndexName(), TimeValue.timeValueSeconds(1));
                }
            }
        }
    }

    /**
     * Only calls DBCollection.count() when using vanilla MongoDB; otherwise gets estimate from collection.getStats()
     */
    private String safeCount(DBCollection collection, Class<? extends Timestamp> type) {
        if (type.equals(Timestamp.BSON.class)) {
            return "" + collection.count(); // Vanilla MongoDB can quickly return precise count
        }
        CommandResult stats = collection.getStats();
        return "~" + (!stats.containsField("count") ? 0l : stats.getLong("count"));
    }

    private BasicDBObject getFilterForInitialImport(BasicDBObject filter, String id) {
        Preconditions.checkNotNull(filter);
        if (id == null) {
            return filter;
        }
        BasicDBObject idFilter = new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, new BasicBSONObject(QueryOperators.GT, id));
        if (filter.equals(new BasicDBObject())) {
            return idFilter;
        }
        return new BasicDBObject(QueryOperators.AND, ImmutableList.of(filter, idFilter));
    }

    private void updateIndexRefresh(String name, Object value) {
        esClient.admin().indices().prepareUpdateSettings(name).setSettings(ImmutableMap.of("index.refresh_interval", value)).get();
    }

    private DBObject applyFieldFilter(DBObject object) {
        if (object instanceof GridFSFile) {
            GridFSFile file = (GridFSFile) object;
            DBObject metadata = file.getMetaData();
            if (metadata != null) {
                file.setMetaData(applyFieldFilter(metadata));
            }
        } else {
            object = MongoDBHelper.applyExcludeFields(object, definition.getExcludeFields());
            object = MongoDBHelper.applyIncludeFields(object, definition.getIncludeFields());
        }
        return object;
    }

    private String addInsertToStream(final Timestamp<?> currentTimestamp, final DBObject data) throws InterruptedException {
        return addInsertToStream(currentTimestamp, data, definition.getMongoCollection());
    }

    private String addInsertToStream(final Timestamp<?> currentTimestamp, final DBObject data, final String collection)
            throws InterruptedException {
        totalDocuments.incrementAndGet();
        addToStream(Operation.INSERT, currentTimestamp, data, collection);
        if (data == null) {
            return null;
        } else {
            return data.containsField(MongoDBRiver.MONGODB_ID_FIELD) ? data.get(MongoDBRiver.MONGODB_ID_FIELD).toString() : null;
        }
    }

    private void addToStream(final Operation operation, final Timestamp<?> currentTimestamp, final DBObject data, final String collection)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            String dataString = data.toString();
            if (dataString.length() > 400) {
                logger.trace("addToStream - operation [{}], currentTimestamp [{}], data (_id:[{}], serialized length:{}), collection [{}]",
                        operation, currentTimestamp, data.get("_id"), dataString.length(), collection);
            } else {
                logger.trace("addToStream - operation [{}], currentTimestamp [{}], data [{}], collection [{}]",
                        operation, currentTimestamp, dataString, collection);
            }
        }

        if (operation == Operation.DROP_DATABASE) {
            logger.info("addToStream - Operation.DROP_DATABASE, currentTimestamp [{}], data [{}], collection [{}]",
                    currentTimestamp, data, collection);
            if (definition.isImportAllCollections()) {
                for (String name : slurpedDb.getCollectionNames()) {
                    logger.info("addToStream - isImportAllCollections - Operation.DROP_DATABASE, currentTimestamp [{}], data [{}], collection [{}]",
                            currentTimestamp, data, name);
                    context.getStream().put(new MongoDBRiver.QueueEntry(currentTimestamp, Operation.DROP_COLLECTION, data, name));
                }
            } else {
                context.getStream().put(new MongoDBRiver.QueueEntry(currentTimestamp, Operation.DROP_COLLECTION, data, collection));
            }
        } else {
            context.getStream().put(new MongoDBRiver.QueueEntry(currentTimestamp, operation, data, collection));
        }
    }

}
