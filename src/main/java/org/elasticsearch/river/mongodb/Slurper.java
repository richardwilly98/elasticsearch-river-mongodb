package org.elasticsearch.river.mongodb;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.CharMatcher;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.QueryOperators;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;
import com.mongodb.util.JSONSerializers;

class Slurper implements Runnable {

    class SlurperException extends Exception {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        SlurperException(String message) {
            super(message);
        }
    }

    private static final ESLogger logger = ESLoggerFactory.getLogger(Slurper.class.getName());

    private final MongoDBRiverDefinition definition;
    private final SharedContext context;
    private final BasicDBObject findKeys;
    private final String gridfsOplogNamespace;
    private final String cmdOplogNamespace;
    private final ImmutableList<String> oplogOperations = ImmutableList.of(MongoDBRiver.OPLOG_DELETE_OPERATION,
            MongoDBRiver.OPLOG_UPDATE_ROW_OPERATION, // from TokuMX
            MongoDBRiver.OPLOG_UPDATE_OPERATION, MongoDBRiver.OPLOG_INSERT_OPERATION, MongoDBRiver.OPLOG_COMMAND_OPERATION);
    private final Client client;
    private Mongo mongo;
    private DB slurpedDb;
    private DB oplogDb;
    private DBCollection oplogCollection, oplogRefsCollection;
    private final AtomicLong totalDocuments = new AtomicLong();

    public Slurper(List<ServerAddress> mongoServers, MongoDBRiverDefinition definition, SharedContext context, Client client) {
        this.definition = definition;
        this.context = context;
        this.client = client;
        this.mongo = new MongoClient(mongoServers, definition.getMongoClientOptions());
        this.findKeys = new BasicDBObject();
        this.gridfsOplogNamespace = definition.getMongoOplogNamespace() + MongoDBRiver.GRIDFS_FILES_SUFFIX;
        this.cmdOplogNamespace = definition.getMongoDb() + "." + MongoDBRiver.OPLOG_NAMESPACE_COMMAND;
        if (definition.getExcludeFields() != null) {
            for (String key : definition.getExcludeFields()) {
                findKeys.put(key, 0);
            }
        } else if (definition.getIncludeFields() != null) {
            for (String key : definition.getIncludeFields()) {
                findKeys.put(key, 1);
            }
        }
    }

    @Override
    public void run() {
        while (context.getStatus() == Status.RUNNING) {
            try {
                if (!assignCollections()) {
                    break; // failed to assign oplogCollection or
                           // slurpedCollection
                }

                Timestamp<?> startTimestamp = null;
                if (!definition.isSkipInitialImport()) {
                    if (!riverHasIndexedFromOplog() && definition.getInitialTimestamp() == null) {
                        if (!isIndexEmpty()) {
                            MongoDBRiverHelper.setRiverStatus(client, definition.getRiverName(), Status.INITIAL_IMPORT_FAILED);
                            break;
                        }
                        if (definition.isImportAllCollections()) {
                            for (String name : slurpedDb.getCollectionNames()) {
                                if (name.length() < 7 || !name.substring(0, 7).equals("system.")) {
                                    DBCollection collection = slurpedDb.getCollection(name);
                                    startTimestamp = doInitialImport(collection);
                                }
                            }
                        } else {
                            DBCollection collection = slurpedDb.getCollection(definition.getMongoCollection());
                            startTimestamp = doInitialImport(collection);
                        }
                    }
                } else {
                    logger.info("Skip initial import from collection {}", definition.getMongoCollection());
                }

                // Slurp from oplog
                DBCursor cursor = null;
                try {
                    cursor = oplogCursor(startTimestamp);
                    if (cursor == null) {
                        cursor = processFullOplog();
                    }
                    while (cursor.hasNext()) {
                        DBObject item = cursor.next();
                        // TokuMX secondaries can have ops in the oplog that
                        // have not yet been applied
                        // We need to wait until they have been applied before
                        // processing them
                        Object applied = item.get("a");
                        if (applied != null && !applied.equals(Boolean.TRUE)) {
                            logger.debug("Encountered oplog entry with a:false, ts:" + item.get("ts"));
                            break;
                        }
                        startTimestamp = processOplogEntry(item, startTimestamp);
                    }
                    logger.debug("Before waiting for 500 ms");
                    Thread.sleep(500);
                } catch (MongoException.CursorNotFound e) {
                    logger.info("Cursor {} has been closed. About to open a new cusor.", cursor.getCursorId());
                    logger.debug("Total document inserted [{}]", totalDocuments.get());
                } catch (SlurperException sEx) {
                    logger.error("Exception in slurper", sEx);
                    break;
                } catch (Exception ex) {
                    logger.error("Exception while looping in cursor", ex);
                    Thread.currentThread().interrupt();
                    break;
                } finally {
                    if (cursor != null) {
                        logger.trace("Closing oplog cursor");
                        cursor.close();
                    }
                }
            } catch (MongoInterruptedException mIEx) {
                logger.warn("Mongo driver has been interrupted", mIEx);
                if (mongo != null) {
                    mongo.close();
                    mongo = null;
                }
                Thread.currentThread().interrupt();
                break;
            } catch (MongoException e) {
                logger.error("Mongo gave an exception", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException iEx) {
                }
            } catch (NoSuchElementException e) {
                logger.warn("A mongoDB cursor bug ?", e);
            } catch (InterruptedException e) {
                logger.info("river-mongodb slurper interrupted");
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    protected boolean riverHasIndexedFromOplog() {
        return MongoDBRiver.getLastTimestamp(client, definition) != null;
    }

    protected boolean isIndexEmpty() {
        return MongoDBRiver.getIndexCount(client, definition) == 0;
    }

    /**
     * Does an initial sync the same way MongoDB does.
     * https://groups.google.com/
     * forum/?fromgroups=#!topic/mongodb-user/sOKlhD_E2ns
     * 
     * @return the last oplog timestamp before the import began
     * @throws InterruptedException
     *             if the blocking queue stream is interrupted while waiting
     */
    protected Timestamp<?> doInitialImport(DBCollection collection) throws InterruptedException {
        // TODO: ensure the index type is empty
        // DBCollection slurpedCollection =
        // slurpedDb.getCollection(definition.getMongoCollection());

        logger.info("MongoDBRiver is beginning initial import of " + collection.getFullName());
        Timestamp<?> startTimestamp = getCurrentOplogTimestamp();
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
                        logger.trace("Collection {} - count: {}", collection.getName(), collection.count());
                    }
                    long count = 0;
                    cursor = collection.find(getFilterForInitialImport(definition.getMongoCollectionFilter(), lastId));
                    while (cursor.hasNext()) {
                        DBObject object = cursor.next();
                        count++;
                        if (cursor.hasNext()) {
                            lastId = addInsertToStream(null, applyFieldFilter(object), collection.getName());
                        } else {
                            logger.debug("Last entry for initial import of {} - add timestamp: {}", collection.getFullName(), startTimestamp);
                            lastId = addInsertToStream(startTimestamp, applyFieldFilter(object), collection.getName());
                        }
                    }
                    inProgress = false;
                    logger.info("Number of documents indexed in initial import of {}: {}", collection.getFullName(), count);
                } else {
                    // TODO: To be optimized.
                    // https://github.com/mongodb/mongo-java-driver/pull/48#issuecomment-25241988
                    // possible option: Get the object id list from .fs
                    // collection
                    // then call GriDFS.findOne
                    GridFS grid = new GridFS(mongo.getDB(definition.getMongoDb()), definition.getMongoCollection());

                    cursor = grid.getFileList();
                    while (cursor.hasNext()) {
                        DBObject object = cursor.next();
                        if (object instanceof GridFSDBFile) {
                            GridFSDBFile file = grid.findOne(new ObjectId(object.get(MongoDBRiver.MONGODB_ID_FIELD).toString()));
                            if (cursor.hasNext()) {
                                lastId = addInsertToStream(null, file);
                            } else {
                                logger.debug("Last entry for initial import of {} - add timestamp: {}", collection.getFullName(), startTimestamp);
                                lastId = addInsertToStream(startTimestamp, file);
                            }
                        }
                    }
                    inProgress = false;
                }
            } catch (MongoException.CursorNotFound e) {
                logger.debug("Initial import - Cursor {} has been closed. About to open a new cursor.", cursor.getCursorId());
                logger.debug("Total documents inserted so far by river {}: {}", definition.getRiverName(), totalDocuments.get());
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
        return startTimestamp;
    }

    private BasicDBObject getFilterForInitialImport(BasicDBObject filter, String id) {
        if (id == null) {
            return filter;
        } else {
            BasicDBObject filterId = new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, new BasicBSONObject(QueryOperators.GT, id));
            if (filter == null) {
                return filterId;
            } else {
                List<BasicDBObject> values = ImmutableList.of(filter, filterId);
                return new BasicDBObject(QueryOperators.AND, values);
            }
        }
    }

    protected boolean assignCollections() {
    	DB adminDb;
    	if(!definition.getMongoAdminAuthDatabase().isEmpty()) {
	    	adminDb = mongo.getDB(definition.getMongoAdminAuthDatabase());
    	} else {
	    	adminDb = mongo.getDB(MongoDBRiver.MONGODB_ADMIN_DATABASE);
    	}
    	
    	if(!definition.getMongoLocalAuthDatabase().isEmpty()) {
    		logger.info("Local DB auth against "+definition.getMongoLocalAuthDatabase()+" user: "+definition.getMongoLocalUser());
	    	oplogDb = mongo.getDB(definition.getMongoLocalAuthDatabase());
    	} else {
    		logger.info("Local DB auth against local user: "+definition.getMongoLocalUser());
	    	oplogDb = mongo.getDB(MongoDBRiver.MONGODB_LOCAL_DATABASE);
    	}

        if (!definition.getMongoAdminUser().isEmpty() && !definition.getMongoAdminPassword().isEmpty()) {
            logger.debug("Authenticate {} with {}", MongoDBRiver.MONGODB_ADMIN_DATABASE, definition.getMongoAdminUser());

            CommandResult cmd = adminDb.authenticateCommand(definition.getMongoAdminUser(), definition.getMongoAdminPassword()
                    .toCharArray());
            if (!cmd.ok()) {
                logger.error("Authentication failed for {}: {}", MongoDBRiver.MONGODB_ADMIN_DATABASE, cmd.getErrorMessage());
                // Can still try with mongoLocal credential if provided.
                // return false;
            }
            oplogDb = adminDb.getMongo().getDB(MongoDBRiver.MONGODB_LOCAL_DATABASE);
        }

        if (!definition.getMongoLocalUser().isEmpty() && !definition.getMongoLocalPassword().isEmpty() && !oplogDb.isAuthenticated()) {
            logger.debug("Authenticate {} with {}", MongoDBRiver.MONGODB_LOCAL_DATABASE, definition.getMongoLocalUser());
            CommandResult cmd = oplogDb.authenticateCommand(definition.getMongoLocalUser(), definition.getMongoLocalPassword()
                    .toCharArray());
            if (!cmd.ok()) {
                logger.error("Authentication failed for {}: {}", MongoDBRiver.MONGODB_LOCAL_DATABASE, cmd.getErrorMessage());
                return false;
            }
            oplogDb = oplogDb.getMongo().getDB(MongoDBRiver.MONGODB_LOCAL_DATABASE);
        }

        Set<String> collections = oplogDb.getCollectionNames();
        if (!collections.contains(MongoDBRiver.OPLOG_COLLECTION)) {
            logger.error("Cannot find " + MongoDBRiver.OPLOG_COLLECTION + " collection. Please check this link: http://goo.gl/2x5IW");
            return false;
        }
        oplogCollection = oplogDb.getCollection(MongoDBRiver.OPLOG_COLLECTION);
        oplogRefsCollection = oplogDb.getCollection(MongoDBRiver.OPLOG_REFS_COLLECTION);

        slurpedDb = mongo.getDB(definition.getMongoDb());
        if (!definition.getMongoAdminUser().isEmpty() && !definition.getMongoAdminPassword().isEmpty() && adminDb.isAuthenticated()) {
            slurpedDb = adminDb.getMongo().getDB(definition.getMongoDb());
        }

        // Not necessary as local user has access to all databases.
        // http://docs.mongodb.org/manual/reference/local-database/
        // if (!mongoDbUser.isEmpty() && !mongoDbPassword.isEmpty()
        // && !slurpedDb.isAuthenticated()) {
        // logger.info("Authenticate {} with {}", mongoDb, mongoDbUser);
        // CommandResult cmd = slurpedDb.authenticateCommand(mongoDbUser,
        // mongoDbPassword.toCharArray());
        // if (!cmd.ok()) {
        // logger.error("Authentication failed for {}: {}",
        // mongoDb, cmd.getErrorMessage());
        // return false;
        // }
        // }
        // slurpedCollection =
        // slurpedDb.getCollection(definition.getMongoCollection());
        // if (definition.isImportAllCollections()) {
        // for (String collection : slurpedDb.getCollectionNames()) {
        // slurpedCollections.put(collection,
        // slurpedDb.getCollection(collection));
        // }
        // } else {
        // slurpedCollections.put(definition.getMongoCollection(),
        // slurpedDb.getCollection(definition.getMongoCollection()));
        // }

        return true;
    }

    private void updateIndexRefresh(String name, Object value) {
        client.admin().indices().prepareUpdateSettings(name).setSettings(ImmutableMap.of("index.refresh_interval", value)).get();
    }

    private Timestamp<?> getCurrentOplogTimestamp() {
        return Timestamp.on(oplogCollection.find().sort(new BasicDBObject(MongoDBRiver.INSERTION_ORDER_KEY, -1)).limit(1).next());
    }

    private DBCursor processFullOplog() throws InterruptedException, SlurperException {
        Timestamp<?> currentTimestamp = getCurrentOplogTimestamp();
        addInsertToStream(currentTimestamp, null);
        return oplogCursor(currentTimestamp);
    }

    private Timestamp<?> processOplogEntry(final DBObject entry, final Timestamp<?> startTimestamp) throws InterruptedException {
        // To support transactions, TokuMX wraps one or more operations in a
        // single oplog entry, in a list.
        // As long as clients are not transaction-aware, we can pretty safely
        // assume there will only be one operation in the list.
        // Supporting genuine multi-operation transactions will require a bit
        // more logic here.
        flattenOps(entry);

        if (!isValidOplogEntry(entry, startTimestamp)) {
            return startTimestamp;
        }
        Operation operation = Operation.fromString(entry.get(MongoDBRiver.OPLOG_OPERATION).toString());
        String namespace = entry.get(MongoDBRiver.OPLOG_NAMESPACE).toString();
        String collection = null;
        Timestamp<?> oplogTimestamp = Timestamp.on(entry);
        DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);

        if (definition.isImportAllCollections()) {
            if (namespace.startsWith(definition.getMongoDb()) && !namespace.equals(cmdOplogNamespace)) {
                collection = getCollectionFromNamespace(namespace);
            }
        } else {
            collection = definition.getMongoCollection();
        }

        if (namespace.equals(cmdOplogNamespace)) {
            if (object.containsField(MongoDBRiver.OPLOG_DROP_COMMAND_OPERATION)) {
                operation = Operation.DROP_COLLECTION;
                if (definition.isImportAllCollections()) {
                    collection = object.get(MongoDBRiver.OPLOG_DROP_COMMAND_OPERATION).toString();
                    if (collection.startsWith("tmp.mr.")) {
                        return startTimestamp;
                    }
                }
            }
            if (object.containsField(MongoDBRiver.OPLOG_DROP_DATABASE_COMMAND_OPERATION)) {
                operation = Operation.DROP_DATABASE;
            }
        }

        logger.trace("namespace: {} - operation: {}", namespace, operation);
        if (namespace.equals(MongoDBRiver.OPLOG_ADMIN_COMMAND)) {
            if (operation == Operation.COMMAND) {
                processAdminCommandOplogEntry(entry, startTimestamp);
                return startTimestamp;
            }
        }

        if (logger.isTraceEnabled()) {
            String deserialized = object.toString();
            if (deserialized.length() < 400) {
                logger.trace("MongoDB object deserialized: {}", deserialized);
            } else {
                logger.trace("MongoDB object deserialized is {} characters long", deserialized.length());
            }
            logger.trace("collection: {}", collection);
            logger.trace("oplog entry - namespace [{}], operation [{}]", namespace, operation);
            if (deserialized.length() < 400) {
                logger.trace("oplog processing item {}", entry);
            }
        }

        String objectId = getObjectIdFromOplogEntry(entry);
        if (operation == Operation.DELETE) {
            // Include only _id in data, as vanilla MongoDB does, so
            // transformation scripts won't be broken by Toku
            if (object.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
                if (object.keySet().size() > 1) {
                    entry.put(MongoDBRiver.OPLOG_OBJECT, object = new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, objectId));
                }
            } else {
                throw new NullPointerException(MongoDBRiver.MONGODB_ID_FIELD);
            }
        }

        if (definition.isMongoGridFS() && namespace.endsWith(MongoDBRiver.GRIDFS_FILES_SUFFIX)
                && (operation == Operation.INSERT || operation == Operation.UPDATE)) {
            if (objectId == null) {
                throw new NullPointerException(MongoDBRiver.MONGODB_ID_FIELD);
            }
            GridFS grid = new GridFS(mongo.getDB(definition.getMongoDb()), collection);
            GridFSDBFile file = grid.findOne(new ObjectId(objectId));
            if (file != null) {
                logger.trace("Caught file: {} - {}", file.getId(), file.getFilename());
                object = file;
            } else {
                logger.error("Cannot find file from id: {}", objectId);
            }
        }

        if (object instanceof GridFSDBFile) {
            if (objectId == null) {
                throw new NullPointerException(MongoDBRiver.MONGODB_ID_FIELD);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Add attachment: {}", objectId);
            }
            addToStream(operation, oplogTimestamp, applyFieldFilter(object), collection);
        } else {
            if (operation == Operation.UPDATE) {
                DBObject update = (DBObject) entry.get(MongoDBRiver.OPLOG_UPDATE);
                logger.trace("Updated item: {}", update);
                addQueryToStream(operation, oplogTimestamp, update, collection);
            } else {
                if (operation == Operation.INSERT) {
                    addInsertToStream(oplogTimestamp, applyFieldFilter(object), collection);
                } else {
                    addToStream(operation, oplogTimestamp, applyFieldFilter(object), collection);
                }
            }
        }
        return oplogTimestamp;
    }

    @SuppressWarnings("unchecked")
    private void flattenOps(DBObject entry) {
        Object ref = entry.removeField(MongoDBRiver.OPLOG_REF);
        Object ops = ref == null ? entry.removeField(MongoDBRiver.OPLOG_OPS) : getRefOps(ref);
        if (ops != null) {
            try {
                for (DBObject op : (List<DBObject>) ops) {
                    String operation = (String) op.get(MongoDBRiver.OPLOG_OPERATION);
                    if (operation.equals(MongoDBRiver.OPLOG_COMMAND_OPERATION)) {
                        DBObject object = (DBObject) op.get(MongoDBRiver.OPLOG_OBJECT);
                        if (object.containsField(MongoDBRiver.OPLOG_CREATE_COMMAND)) {
                            continue;
                        }
                    }
                    entry.putAll(op);
                }
            } catch (ClassCastException e) {
                logger.error(e.toString(), e);
            }
        }
    }

    private Object getRefOps(Object ref) {
        // db.oplog.refs.find({_id: {$gte: {oid: %ref%}}}).limit(1)
        DBObject query = new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, new BasicDBObject(QueryOperators.GTE,
                new BasicDBObject(MongoDBRiver.MONGODB_OID_FIELD, ref)));
        DBObject oplog = oplogRefsCollection.findOne(query);
        return oplog == null ? null : oplog.get("ops");
    }

    private void processAdminCommandOplogEntry(final DBObject entry, final Timestamp<?> startTimestamp) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("processAdminCommandOplogEntry - [{}]", entry);
        }
        DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);
        if (definition.isImportAllCollections()) {
            if (object.containsField(MongoDBRiver.OPLOG_RENAME_COLLECTION_COMMAND_OPERATION) && object.containsField(MongoDBRiver.OPLOG_TO)) {
                String to = object.get(MongoDBRiver.OPLOG_TO).toString();
                if (to.startsWith(definition.getMongoDb())) {
                    String newCollection = getCollectionFromNamespace(to);
                    DBCollection coll = slurpedDb.getCollection(newCollection);
                    doInitialImport(coll);
                }
            }
        }
    }

    private String getCollectionFromNamespace(String namespace) {
        if (namespace.startsWith(definition.getMongoDb()) && CharMatcher.is('.').countIn(namespace) == 1) {
            return namespace.substring(definition.getMongoDb().length() + 1);
        }
        logger.error("Cannot get collection from namespace [{}]", namespace);
        return null;
    }

    private boolean isValidOplogEntry(final DBObject entry, final Timestamp<?> startTimestamp) {
        if (!entry.containsField(MongoDBRiver.OPLOG_OPERATION)) {
            logger.trace("[Empty Oplog Entry] - can be ignored. {}", JSONSerializers.getStrict().serialize(entry));
            return false;
        }
        if (MongoDBRiver.OPLOG_NOOP_OPERATION.equals(entry.get(MongoDBRiver.OPLOG_OPERATION))) {
            logger.trace("[No-op Oplog Entry] - can be ignored. {}", JSONSerializers.getStrict().serialize(entry));
            return false;
        }
        String namespace = (String) entry.get(MongoDBRiver.OPLOG_NAMESPACE);
        // Initial support for sharded collection -
        // https://jira.mongodb.org/browse/SERVER-4333
        // Not interested in operation from migration or sharding
        if (entry.containsField(MongoDBRiver.OPLOG_FROM_MIGRATE) && ((BasicBSONObject) entry).getBoolean(MongoDBRiver.OPLOG_FROM_MIGRATE)) {
            logger.trace("[Invalid Oplog Entry] - from migration or sharding operation. Can be ignored. {}", JSONSerializers.getStrict().serialize(entry));
            return false;
        }
        // Not interested by chunks - skip all
        if (namespace.endsWith(MongoDBRiver.GRIDFS_CHUNKS_SUFFIX)) {
            return false;
        }

        if (startTimestamp != null) {
            Timestamp<?> oplogTimestamp = Timestamp.on(entry);
            if (Timestamp.compare(oplogTimestamp, startTimestamp) < 0) {
                logger.error("[Invalid Oplog Entry] - entry timestamp [{}] before startTimestamp [{}]",
                        JSONSerializers.getStrict().serialize(entry), startTimestamp);
                return false;
            }
        }

        boolean validNamespace = false;
        if (definition.isMongoGridFS()) {
            validNamespace = gridfsOplogNamespace.equals(namespace);
        } else {
            if (definition.isImportAllCollections()) {
                // Skip temp entry generated by map / reduce
                if (namespace.startsWith(definition.getMongoDb()) && !namespace.startsWith(definition.getMongoDb() + ".tmp.mr")) {
                    validNamespace = true;
                }
            } else {
                if (definition.getMongoOplogNamespace().equals(namespace)) {
                    validNamespace = true;
                }
            }
            if (cmdOplogNamespace.equals(namespace)) {
                validNamespace = true;
            }

            if (MongoDBRiver.OPLOG_ADMIN_COMMAND.equals(namespace)) {
                validNamespace = true;
            }
        }
        if (!validNamespace) {
            logger.trace("[Invalid Oplog Entry] - namespace [{}] is not valid", namespace);
            return false;
        }
        String operation = (String) entry.get(MongoDBRiver.OPLOG_OPERATION);
        if (!oplogOperations.contains(operation)) {
            logger.trace("[Invalid Oplog Entry] - operation [{}] is not valid", operation);
            return false;
        }

        // TODO: implement a better solution
        if (definition.getMongoOplogFilter() != null) {
            DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);
            BasicDBObject filter = definition.getMongoOplogFilter();
            if (!filterMatch(filter, object)) {
                logger.trace("[Invalid Oplog Entry] - filter [{}] does not match object [{}]", filter, object);
                return false;
            }
        }
        return true;
    }

    private boolean filterMatch(DBObject filter, DBObject object) {
        for (String key : filter.keySet()) {
            if (!object.containsField(key)) {
                return false;
            }
            if (!filter.get(key).equals(object.get(key))) {
                return false;
            }
        }
        return true;
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

    /*
     * Extract "_id" from "o" if it fails try to extract from "o2"
     */
    private String getObjectIdFromOplogEntry(DBObject entry) {
        if (entry.containsField(MongoDBRiver.OPLOG_OBJECT)) {
            DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);
            if (object.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
                return object.get(MongoDBRiver.MONGODB_ID_FIELD).toString();
            }
        }
        if (entry.containsField(MongoDBRiver.OPLOG_UPDATE)) {
            DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_UPDATE);
            if (object.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
                return object.get(MongoDBRiver.MONGODB_ID_FIELD).toString();
            }
        }
        return null;
    }

    private DBCursor oplogCursor(final Timestamp<?> timestampOverride) throws SlurperException {
        Timestamp<?> time = timestampOverride == null ? MongoDBRiver.getLastTimestamp(client, definition) : timestampOverride;
        if (time == null) {
            return null;
        }
        DBObject indexFilter = time.getOplogFilter();
        if (indexFilter == null) {
            return null;
        }

        int options = Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA | Bytes.QUERYOPTION_NOTIMEOUT
        // Using OPLOGREPLAY to improve performance:
        // https://jira.mongodb.org/browse/JAVA-771
                | Bytes.QUERYOPTION_OPLOGREPLAY;

        DBCursor cursor = oplogCollection.find(indexFilter).setOptions(options);

        // Toku sometimes gets stuck without this hint:
        if (indexFilter.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
            cursor = cursor.hint("_id_");
        }
        isRiverStale(cursor, time);
        return cursor;
    }

    private void isRiverStale(DBCursor cursor, Timestamp<?> time) throws SlurperException {
        if (cursor == null || time == null) {
            return;
        }
        if (definition.getInitialTimestamp() != null && time.equals(definition.getInitialTimestamp())) {
            return;
        }
        DBObject entry = cursor.next();
        Timestamp<?> oplogTimestamp = Timestamp.on(entry);
        if (!time.equals(oplogTimestamp)) {
            MongoDBRiverHelper.setRiverStatus(client, definition.getRiverName(), Status.RIVER_STALE);
            throw new SlurperException("River out of sync with oplog.rs collection");
        }
    }

    private void addQueryToStream(final Operation operation, final Timestamp<?> currentTimestamp, final DBObject update,
            final String collection) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("addQueryToStream - operation [{}], currentTimestamp [{}], update [{}]", operation, currentTimestamp, update);
        }

        if (collection == null) {
            for (String name : slurpedDb.getCollectionNames()) {
                DBCollection slurpedCollection = slurpedDb.getCollection(name);
                for (DBObject item : slurpedCollection.find(update, findKeys)) {
                    addToStream(operation, currentTimestamp, item, collection);
                }
            }
        } else {
            DBCollection slurpedCollection = slurpedDb.getCollection(collection);
            for (DBObject item : slurpedCollection.find(update, findKeys)) {
                addToStream(operation, currentTimestamp, item, collection);
            }
        }
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
            logger.info("addToStream - Operation.DROP_DATABASE, currentTimestamp [{}], data [{}], collection [{}]", currentTimestamp,
                    data, collection);
            if (definition.isImportAllCollections()) {
                for (String name : slurpedDb.getCollectionNames()) {
                    logger.info("addToStream - isImportAllCollections - Operation.DROP_DATABASE, currentTimestamp [{}], data [{}], collection [{}]", currentTimestamp,
                            data, name);
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
