package org.elasticsearch.river.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;

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
import com.mongodb.util.JSON;

class Slurper implements Runnable {

	private static final ESLogger logger = ESLoggerFactory.getLogger(Slurper.class.getName());

	private final MongoDBRiverDefinition definition;
    private final SharedContext context;
    private final BasicDBObject findKeys;
    private final Client client;
	private Mongo mongo;
	private DB slurpedDb;
	private DBCollection slurpedCollection;
	private DB oplogDb;
	private DBCollection oplogCollection;

	public Slurper(
			List<ServerAddress> mongoServers,
			MongoDBRiverDefinition definition,
			SharedContext context,
			Client client) {
		this.definition = definition;
		this.context = context;
		this.client = client;
		this.mongo = new MongoClient(mongoServers, definition.getMongoClientOptions());
		this.findKeys = new BasicDBObject();
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
		while (context.isActive()) {
			try {
				if (!assignCollections()) {
					break;	// failed to assign oplogCollection or
							// slurpedCollection
				}

				BSONTimestamp startTimestamp = null;
				if (!riverHasIndexedSomething()) {
					startTimestamp = doInitialImport();
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
						processOplogEntry(item);
					}
					Thread.sleep(500);
				} finally {
					if (cursor != null) {
						logger.trace("Closing oplog cursor");
						cursor.close();
					}
				}
			} catch (MongoInterruptedException mIEx) {
				logger.warn("Mongo driver has been interrupted");
				if (mongo != null) {
					mongo.close();
					mongo = null;
				}
				break;
			} catch (MongoException e) {
				logger.error("Mongo gave an exception", e);
			} catch (NoSuchElementException e) {
				logger.warn("A mongoDB cursor bug ?", e);
			} catch (InterruptedException e) {
				logger.debug("river-mongodb slurper interrupted");
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	protected boolean riverHasIndexedSomething() {
		return MongoDBRiver.getLastTimestamp(client, definition) != null;
	}

	/**
	 * Does an initial sync the same way MongoDB does.
	 * https://groups.google.com/forum/?fromgroups=#!topic/mongodb-user/sOKlhD_E2ns
	 * @return the last oplog timestamp before the import began 
	 * @throws InterruptedException if the blocking queue stream is interrupted while waiting
	 */
	protected BSONTimestamp doInitialImport() throws InterruptedException {
		// TODO: support gridfs
		if (!definition.isMongoGridFS()) {
			// TODO: ensure the index type is empty
			logger.info("MongoDBRiver is beginning initial import of "
				+ slurpedCollection.getFullName());
			BSONTimestamp startTimestamp = getCurrentOplogTimestamp();
			DBCursor cursor = null;
			try {
				cursor = slurpedCollection.find();
				while (cursor.hasNext()) {
					DBObject object = cursor.next();
					Map<String, Object> map = applyFieldFilter(object).toMap();
					addToStream(MongoDBRiver.OPLOG_INSERT_OPERATION, null, map);
				}	
			} finally {
				if (cursor != null) {
					logger.trace("Closing initial import cursor");
					cursor.close();
				}
			}
			return startTimestamp;
		}
		return null;
	}

	protected boolean assignCollections() {
		DB adminDb = mongo.getDB(MongoDBRiver.MONGODB_ADMIN_DATABASE);
		oplogDb = mongo.getDB(MongoDBRiver.MONGODB_LOCAL_DATABASE);

		if (!definition.getMongoAdminUser().isEmpty()
				&& !definition.getMongoAdminPassword().isEmpty()) {
			logger.info("Authenticate {} with {}", MongoDBRiver.MONGODB_ADMIN_DATABASE,
					definition.getMongoAdminUser());

			CommandResult cmd = adminDb.authenticateCommand(definition
					.getMongoAdminUser(), definition
					.getMongoAdminPassword().toCharArray());
			if (!cmd.ok()) {
				logger.error("Autenticatication failed for {}: {}",
						MongoDBRiver.MONGODB_ADMIN_DATABASE, cmd.getErrorMessage());
				// Can still try with mongoLocal credential if provided.
				// return false;
			}
			oplogDb = adminDb.getMongo().getDB(MongoDBRiver.MONGODB_LOCAL_DATABASE);
		}

		if (!definition.getMongoLocalUser().isEmpty()
				&& !definition.getMongoLocalPassword().isEmpty()
				&& !oplogDb.isAuthenticated()) {
			logger.info("Authenticate {} with {}", MongoDBRiver.MONGODB_LOCAL_DATABASE,
					definition.getMongoLocalUser());
			CommandResult cmd = oplogDb.authenticateCommand(definition
					.getMongoLocalUser(), definition
					.getMongoLocalPassword().toCharArray());
			if (!cmd.ok()) {
				logger.error("Autenticatication failed for {}: {}",
						MongoDBRiver.MONGODB_LOCAL_DATABASE, cmd.getErrorMessage());
				return false;
			}
		}

		Set<String> collections = oplogDb.getCollectionNames();
		if (!collections.contains(MongoDBRiver.OPLOG_COLLECTION)) {
			logger.error("Cannot find "
					+ MongoDBRiver.OPLOG_COLLECTION
					+ " collection. Please check this link: http://goo.gl/2x5IW");
			return false;
		}
		oplogCollection = oplogDb.getCollection(MongoDBRiver.OPLOG_COLLECTION);

		slurpedDb = mongo.getDB(definition.getMongoDb());
		if (!definition.getMongoAdminUser().isEmpty()
				&& !definition.getMongoAdminPassword().isEmpty()
				&& adminDb.isAuthenticated()) {
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
		slurpedCollection = slurpedDb.getCollection(definition
				.getMongoCollection());

		return true;
	}

	private BSONTimestamp getCurrentOplogTimestamp() {
		return (BSONTimestamp) oplogCollection
				.find()
				.sort(new BasicDBObject(MongoDBRiver.OPLOG_TIMESTAMP, -1))
				.limit(1)
				.next()
				.get(MongoDBRiver.OPLOG_TIMESTAMP);
	}

	private DBCursor processFullOplog() throws InterruptedException {
		BSONTimestamp currentTimestamp = getCurrentOplogTimestamp();
		addQueryToStream(MongoDBRiver.OPLOG_INSERT_OPERATION, currentTimestamp, null);
		return oplogCursor(currentTimestamp);
	}

	@SuppressWarnings("unchecked")
	private void processOplogEntry(final DBObject entry)
			throws InterruptedException {
		String operation = entry.get(MongoDBRiver.OPLOG_OPERATION).toString();
		String namespace = entry.get(MongoDBRiver.OPLOG_NAMESPACE).toString();
		BSONTimestamp oplogTimestamp = (BSONTimestamp) entry
				.get(MongoDBRiver.OPLOG_TIMESTAMP);
		DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);

		if (logger.isTraceEnabled()) {
			logger.trace("MongoDB object deserialized: {}",
					object.toString());
		}

		// Initial support for sharded collection -
		// https://jira.mongodb.org/browse/SERVER-4333
		// Not interested in operation from migration or sharding
		if (entry.containsField(MongoDBRiver.OPLOG_FROM_MIGRATE)
				&& ((BasicBSONObject) entry).getBoolean(MongoDBRiver.OPLOG_FROM_MIGRATE)) {
			logger.debug(
					"From migration or sharding operation. Can be ignored. {}",
					entry);
			return;
		}
		// Not interested by chunks - skip all
		if (namespace.endsWith(MongoDBRiver.GRIDFS_CHUNKS_SUFFIX)) {
			return;
		}

		if (logger.isTraceEnabled()) {
			logger.trace("oplog entry - namespace [{}], operation [{}]",
					namespace, operation);
			logger.trace("oplog processing item {}", entry);
		}

		String objectId = getObjectIdFromOplogEntry(entry);
		if (definition.isMongoGridFS()
				&& namespace.endsWith(MongoDBRiver.GRIDFS_FILES_SUFFIX)
				&& (MongoDBRiver.OPLOG_INSERT_OPERATION.equals(operation) || MongoDBRiver.OPLOG_UPDATE_OPERATION
						.equals(operation))) {
			if (objectId == null) {
				throw new NullPointerException(MongoDBRiver.MONGODB_ID_FIELD);
			}
			GridFS grid = new GridFS(mongo.getDB(definition.getMongoDb()),
					definition.getMongoCollection());
			GridFSDBFile file = grid.findOne(new ObjectId(objectId));
			if (file != null) {
				logger.info("Caught file: {} - {}", file.getId(),
						file.getFilename());
				object = file;
			} else {
				logger.warn("Cannot find file from id: {}", objectId);
			}
		}

		if (object instanceof GridFSDBFile) {
			if (objectId == null) {
				throw new NullPointerException(MongoDBRiver.MONGODB_ID_FIELD);
			}
			logger.info("Add attachment: {}", objectId);
			object = applyFieldFilter(object);
			HashMap<String, Object> data = new HashMap<String, Object>();
			data.put(MongoDBRiver.IS_MONGODB_ATTACHMENT, true);
			data.put(MongoDBRiver.MONGODB_ATTACHMENT, object);
			data.put(MongoDBRiver.MONGODB_ID_FIELD, objectId);
			addToStream(operation, oplogTimestamp, data);
		} else {
			if (MongoDBRiver.OPLOG_UPDATE_OPERATION.equals(operation)) {
				DBObject update = (DBObject) entry.get(MongoDBRiver.OPLOG_UPDATE);
				logger.debug("Updated item: {}", update);
				addQueryToStream(operation, oplogTimestamp, update);
			} else {
				Map<String, Object> map = applyFieldFilter(object).toMap();
				addToStream(operation, oplogTimestamp, map);
			}
		}
	}

	private DBObject applyFieldFilter(DBObject object) {
		object = MongoDBHelper.applyExcludeFields(object,
				definition.getExcludeFields());
		object = MongoDBHelper.applyIncludeFields(object,
				definition.getIncludeFields());
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
		logger.trace("Oplog entry {}", entry);
		return null;
	}

	private DBObject getOplogFilter(final BSONTimestamp time) {
		BasicDBObject filter = new BasicDBObject();
		List<DBObject> values2 = new ArrayList<DBObject>();

		if (time == null) {
			logger.info("No known previous slurping time for this collection");
		} else {
			filter.put(MongoDBRiver.OPLOG_TIMESTAMP, new BasicDBObject(
					QueryOperators.GT, time));
		}

		if (definition.isMongoGridFS()) {
			filter.put(MongoDBRiver.OPLOG_NAMESPACE, definition.getMongoOplogNamespace()
					+ MongoDBRiver.GRIDFS_FILES_SUFFIX);
		} else {
			values2.add(new BasicDBObject(MongoDBRiver.OPLOG_NAMESPACE,
					definition.getMongoOplogNamespace()));
			values2.add(new BasicDBObject(MongoDBRiver.OPLOG_NAMESPACE, definition
					.getMongoDb() + "." + MongoDBRiver.OPLOG_NAMESPACE_COMMAND));
			filter.put(MongoDBRiver.MONGODB_OR_OPERATOR, values2);
		}
		if (!definition.getMongoFilter().isEmpty()) {
			filter.putAll(getMongoFilter());
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Using filter: {}", filter);
		}
		return filter;
	}

	private DBObject getMongoFilter() {
		List<DBObject> filters = new ArrayList<DBObject>();
		List<DBObject> filters2 = new ArrayList<DBObject>();
		List<DBObject> filters3 = new ArrayList<DBObject>();
		// include delete operation
		filters.add(new BasicDBObject(MongoDBRiver.OPLOG_OPERATION,
				MongoDBRiver.OPLOG_DELETE_OPERATION));

		// include update, insert in filters3
		filters3.add(new BasicDBObject(MongoDBRiver.OPLOG_OPERATION,
				MongoDBRiver.OPLOG_UPDATE_OPERATION));
		filters3.add(new BasicDBObject(MongoDBRiver.OPLOG_OPERATION,
				MongoDBRiver.OPLOG_INSERT_OPERATION));

		// include or operation statement in filter2
		filters2.add(new BasicDBObject(MongoDBRiver.MONGODB_OR_OPERATOR, filters3));

		// include custom filter in filters2
		filters2.add((DBObject) JSON.parse(definition.getMongoFilter()));

		filters.add(new BasicDBObject(MongoDBRiver.MONGODB_AND_OPERATOR, filters2));

		return new BasicDBObject(MongoDBRiver.MONGODB_OR_OPERATOR, filters);
	}

	private DBCursor oplogCursor(final BSONTimestamp timestampOverride) {
		BSONTimestamp time = timestampOverride == null
				? MongoDBRiver.getLastTimestamp(client, definition) : timestampOverride;
		DBObject indexFilter = getOplogFilter(time);
		if (indexFilter == null) {
			return null;
		}

		int options = Bytes.QUERYOPTION_TAILABLE
				| Bytes.QUERYOPTION_AWAITDATA | Bytes.QUERYOPTION_NOTIMEOUT;

		// Using OPLOGREPLAY to improve performance:
		// https://jira.mongodb.org/browse/JAVA-771
		if (indexFilter.containsField(MongoDBRiver.OPLOG_TIMESTAMP)) {
			options = options | Bytes.QUERYOPTION_OPLOGREPLAY;
		}
		return oplogCollection.find(indexFilter)
				.sort(new BasicDBObject(MongoDBRiver.MONGODB_NATURAL_OPERATOR, 1))
				.setOptions(options);
	}

	@SuppressWarnings("unchecked")
	private void addQueryToStream(final String operation,
			final BSONTimestamp currentTimestamp, final DBObject update)
			throws InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug(
					"addQueryToStream - operation [{}], currentTimestamp [{}], update [{}]",
					operation, currentTimestamp, update);
		}

		for (DBObject item : slurpedCollection.find(update, findKeys)) {
			addToStream(operation, currentTimestamp, item.toMap());
		}
	}

	private void addToStream(final String operation,
			final BSONTimestamp currentTimestamp,
			final Map<String, Object> data) throws InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug(
					"addToStream - operation [{}], currentTimestamp [{}], data [{}]",
					operation, currentTimestamp, data);
		}

		context.getStream().put(new MongoDBRiver.QueueEntry(currentTimestamp, operation, data));
	}

}