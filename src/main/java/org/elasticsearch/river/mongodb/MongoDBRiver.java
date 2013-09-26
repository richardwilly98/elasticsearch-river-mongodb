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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

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

/**
 * @author richardwilly98 (Richard Louapre)
 * @author flaper87 (Flavio Percoco Premoli)
 * @author aparo (Alberto Paro)
 * @author kryptt (Rodolfo Hansen)
 */
public class MongoDBRiver extends AbstractRiverComponent implements River {

	public final static String IS_MONGODB_ATTACHMENT = "is_mongodb_attachment";
	public final static String MONGODB_ATTACHMENT = "mongodb_attachment";
	public final static String TYPE = "mongodb";
	public final static String NAME = "mongodb-river";
	public final static String STATUS = "_mongodbstatus";
	public final static String ENABLED = "enabled";
	public final static String DESCRIPTION = "MongoDB River Plugin";
	public final static String LAST_TIMESTAMP_FIELD = "_last_ts";
	public final static String MONGODB_LOCAL_DATABASE = "local";
	public final static String MONGODB_ADMIN_DATABASE = "admin";
	public final static String MONGODB_CONFIG_DATABASE = "config";
	public final static String MONGODB_ID_FIELD = "_id";
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
	public final static String OPLOG_TIMESTAMP = "ts";
	public final static String OPLOG_FROM_MIGRATE = "fromMigrate";
	public final static String GRIDFS_FILES_SUFFIX = ".files";
	public final static String GRIDFS_CHUNKS_SUFFIX = ".chunks";

	protected final Client client;
	protected final String riverIndexName;
	protected final ScriptService scriptService;

	protected final MongoDBRiverDefinition definition;
	protected final String mongoOplogNamespace;

	protected final BasicDBObject findKeys = new BasicDBObject();

	protected volatile List<Thread> tailerThreads = new ArrayList<Thread>();
	protected volatile Thread indexerThread;
	protected volatile Thread statusThread;
	protected volatile boolean active = false;
	protected volatile boolean startInvoked = false;

	private final BlockingQueue<QueueEntry> stream;

	private Mongo mongo;
	private DB adminDb;

	@Inject
	public MongoDBRiver(final RiverName riverName,
			final RiverSettings settings,
			@RiverIndexName final String riverIndexName, final Client client,
			final ScriptService scriptService) {
		super(riverName, settings);
		if (logger.isDebugEnabled()) {
			logger.debug("Prefix: [{}] - name: [{}]", logger.getPrefix(),
					logger.getName());
		}
		this.scriptService = scriptService;
		this.riverIndexName = riverIndexName;
		this.client = client;

		this.definition = MongoDBRiverDefinition.parseSettings(riverName,
				settings, scriptService);

		if (definition.getExcludeFields() != null) {
			for (String key : definition.getExcludeFields()) {
				findKeys.put(key, 0);
			}
		} else if (definition.getIncludeFields() != null) {
			for (String key : definition.getIncludeFields()) {
				findKeys.put(key, 1);
			}
		}
		mongoOplogNamespace = definition.getMongoDb() + "."
				+ definition.getMongoCollection();

		if (definition.getThrottleSize() == -1) {
			stream = new LinkedTransferQueue<QueueEntry>();
		} else {
			stream = new ArrayBlockingQueue<QueueEntry>(
					definition.getThrottleSize());
		}

		statusThread = EsExecutors.daemonThreadFactory(
				settings.globalSettings(), "mongodb_river_status").newThread(
				new Status());
		statusThread.start();
	}

	public void start() {
		if (!MongoDBRiverHelper.isRiverEnabled(client, riverName.getName())) {
			logger.debug("Cannot start river {}. It is currently disabled",
					riverName.getName());
			startInvoked = true;
			return;
		}
		active = true;
		for (ServerAddress server : definition.getMongoServers()) {
			logger.info("Using mongodb server(s): host [{}], port [{}]",
					server.getHost(), server.getPort());
		}
		// http://stackoverflow.com/questions/5270611/read-maven-properties-file-inside-jar-war-file
		logger.info("{} version: [{}]", DESCRIPTION,
				MongoDBHelper.getRiverVersion());
		logger.info(
				"starting mongodb stream. options: secondaryreadpreference [{}], drop_collection [{}], include_collection [{}], throttlesize [{}], gridfs [{}], filter [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]",
				definition.isMongoSecondaryReadPreference(),
				definition.isDropCollection(),
				definition.getIncludeCollection(),
				definition.getThrottleSize(), definition.isMongoGridFS(),
				definition.getMongoFilter(), definition.getMongoDb(),
				definition.getMongoCollection(), definition.getScript(),
				definition.getIndexName(), definition.getTypeName());

		// Create the index if it does not exist
		try {
			client.admin().indices().prepareCreate(definition.getIndexName())
					.execute().actionGet();
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// that's fine
			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ok, not recovered yet..., lets start indexing and hope we
				// recover by the first bulk
				// TODO: a smarter logic can be to register for cluster event
				// listener here, and only start sampling when the
				// block is removed...
			} else {
				logger.warn("failed to create index [{}], disabling river...",
						e, definition.getIndexName());
				return;
			}
		}

		// GridFS
		if (definition.isMongoGridFS()) {
			try {
				if (logger.isDebugEnabled()) {
					logger.debug("Set explicit attachment mapping.");
				}
				client.admin().indices()
						.preparePutMapping(definition.getIndexName())
						.setType(definition.getTypeName())
						.setSource(getGridFSMapping()).execute().actionGet();
			} catch (Exception e) {
				logger.warn("Failed to set explicit mapping (attachment): {}",
						e);
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
						String replicaName = item.get(MONGODB_ID_FIELD)
								.toString();
						Thread tailerThread = EsExecutors.daemonThreadFactory(
								settings.globalSettings(),
								"mongodb_river_slurper-" + replicaName)
								.newThread(new Slurper(servers));
						tailerThreads.add(tailerThread);
					}
				}
			} finally {
				cursor.close();
			}
		} else {
			Thread tailerThread = EsExecutors.daemonThreadFactory(
					settings.globalSettings(), "mongodb_river_slurper")
					.newThread(new Slurper(definition.getMongoServers()));
			tailerThreads.add(tailerThread);
		}

		for (Thread thread : tailerThreads) {
			thread.start();
		}

		indexerThread = EsExecutors.daemonThreadFactory(
				settings.globalSettings(), "mongodb_river_indexer").newThread(
				new Indexer());
		indexerThread.start();

		startInvoked = true;
	}

	private boolean isMongos() {
		DB adminDb = getAdminDb();
		if (adminDb == null) {
			return false;
		}
		CommandResult cr = adminDb
				.command(new BasicDBObject("serverStatus", 1));
		if (cr == null || cr.get("process") == null) {
			logger.warn("serverStatus return null.");
			return false;
		}
		String process = cr.get("process").toString().toLowerCase();
		if (logger.isTraceEnabled()) {
			logger.trace("serverStatus: {}", cr);
			logger.trace("process: {}", process);
		}
		// Fix for https://jira.mongodb.org/browse/SERVER-9160
		return (process.contains("mongos"));
	}

	private DB getAdminDb() {
		if (adminDb == null) {
			adminDb = getMongoClient().getDB(MONGODB_ADMIN_DATABASE);
			if (!definition.getMongoAdminUser().isEmpty()
					&& !definition.getMongoAdminPassword().isEmpty()
					&& !adminDb.isAuthenticated()) {
				logger.info("Authenticate {} with {}", MONGODB_ADMIN_DATABASE,
						definition.getMongoAdminUser());

				try {
					CommandResult cmd = adminDb.authenticateCommand(definition
							.getMongoAdminUser(), definition
							.getMongoAdminPassword().toCharArray());
					if (!cmd.ok()) {
						logger.error("Autenticatication failed for {}: {}",
								MONGODB_ADMIN_DATABASE, cmd.getErrorMessage());
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
		if (!definition.getMongoAdminUser().isEmpty()
				&& !definition.getMongoAdminPassword().isEmpty()
				&& getAdminDb().isAuthenticated()) {
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
			mongo = new MongoClient(definition.getMongoServers(),
					definition.getMongoClientOptions());
		}
		return mongo;
	}

	private void closeMongoClient() {
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
			logger.debug("getServerAddressForReplica - definition: {}",
					definition);
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
		logger.info("closing mongodb stream river");
		try {
			for (Thread thread : tailerThreads) {
				thread.interrupt();
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
			active = false;
		}
	}

	private class Indexer implements Runnable {

		private final ESLogger logger = ESLoggerFactory.getLogger(this
				.getClass().getName());
		private int deletedDocuments = 0;
		private int insertedDocuments = 0;
		private int updatedDocuments = 0;
		private StopWatch sw;
		private ExecutableScript scriptExecutable;

		@Override
		public void run() {
			while (active) {
				sw = new StopWatch().start();
				deletedDocuments = 0;
				insertedDocuments = 0;
				updatedDocuments = 0;

				if (definition.getScript() != null
						&& definition.getScriptType() != null) {
					scriptExecutable = scriptService.executable(
							definition.getScriptType(), definition.getScript(),
							ImmutableMap.of("logger", logger));
				}

				try {
					BSONTimestamp lastTimestamp = null;
					BulkRequestBuilder bulk = client.prepareBulk();

					// 1. Attempt to fill as much of the bulk request as possible
					QueueEntry entry = stream.take();
					lastTimestamp = processBlockingQueue(bulk, entry);
					while ((entry = stream.poll(definition.getBulkTimeout()
							.millis(), MILLISECONDS)) != null) {
						lastTimestamp = processBlockingQueue(bulk, entry);
						if (bulk.numberOfActions() >= definition.getBulkSize()) {
							break;
						}
					}

					// 2. Update the timestamp
					if (lastTimestamp != null) {
						updateLastTimestamp(mongoOplogNamespace, lastTimestamp, bulk);
					}

					// 3. Execute the bulk requests
					try {
						BulkResponse response = bulk.execute().actionGet();
						if (response.hasFailures()) {
							// TODO write to exception queue?
							logger.warn("failed to execute"
									+ response.buildFailureMessage());
						}
					} catch (ElasticSearchInterruptedException esie) {
						logger.warn(
								"river-mongodb indexer bas been interrupted",
								esie);
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						logger.warn("failed to execute bulk", e);
					}

				} catch (InterruptedException e) {
					if (logger.isDebugEnabled()) {
						logger.debug("river-mongodb indexer interrupted");
					}
					Thread.currentThread().interrupt();
					break;
				}
				logStatistics();
			}
		}

		@SuppressWarnings({ "unchecked" })
		private BSONTimestamp processBlockingQueue(
				final BulkRequestBuilder bulk, QueueEntry entry) {
			if (entry.getData().get(MONGODB_ID_FIELD) == null
					&& !entry.getOperation().equals(
							OPLOG_COMMAND_OPERATION)) {
				logger.warn(
						"Cannot get object id. Skip the current item: [{}]",
						entry.getData());
				return null;
			}

			BSONTimestamp lastTimestamp = entry.getOplogTimestamp();
			String operation = entry.getOperation();
			if (OPLOG_COMMAND_OPERATION.equals(operation)) {
				try {
					updateBulkRequest(bulk, entry.getData(), null, operation,
							definition.getIndexName(),
							definition.getTypeName(), null, null);
				} catch (IOException ioEx) {
					logger.error("Update bulk failed.", ioEx);
				}
				return lastTimestamp;
			}

			if (scriptExecutable != null
					&& definition.isAdvancedTransformation()) {
				return applyAdvancedTransformation(bulk, entry);
			}

			String objectId = "";
			if (entry.getData().get(MONGODB_ID_FIELD) != null) {
				objectId = entry.getData().get(MONGODB_ID_FIELD).toString();
			}

			if (logger.isDebugEnabled()) {
				logger.debug("updateBulkRequest for id: [{}], operation: [{}]",
						objectId, operation);
			}

			if (!definition.getIncludeCollection().isEmpty()) {
				logger.trace(
						"About to include collection. set attribute {} / {} ",
						definition.getIncludeCollection(),
						definition.getMongoCollection());
				entry.getData().put(definition.getIncludeCollection(),
						definition.getMongoCollection());
			}

			Map<String, Object> ctx = null;
			try {
				ctx = XContentFactory.xContent(XContentType.JSON)
						.createParser("{}").mapAndClose();
			} catch (IOException e) {
				logger.warn("failed to parse {}", e);
			}
			Map<String, Object> data = entry.getData();
			if (scriptExecutable != null) {
				if (ctx != null) {
					ctx.put("document", entry.getData());
					ctx.put("operation", operation);
					if (!objectId.isEmpty()) {
						ctx.put("id", objectId);
					}
					if (logger.isDebugEnabled()) {
						logger.debug("Script to be executed: {}",
								scriptExecutable);
						logger.debug("Context before script executed: {}", ctx);
					}
					scriptExecutable.setNextVar("ctx", ctx);
					try {
						scriptExecutable.run();
						// we need to unwrap the context object...
						ctx = (Map<String, Object>) scriptExecutable
								.unwrap(ctx);
					} catch (Exception e) {
						logger.warn("failed to script process {}, ignoring", e,
								ctx);
					}
					if (logger.isDebugEnabled()) {
						logger.debug("Context after script executed: {}", ctx);
					}
					if (ctx.containsKey("ignore")
							&& ctx.get("ignore").equals(Boolean.TRUE)) {
						logger.debug("From script ignore document id: {}",
								objectId);
						// ignore document
						return lastTimestamp;
					}
					if (ctx.containsKey("deleted")
							&& ctx.get("deleted").equals(Boolean.TRUE)) {
						ctx.put("operation", OPLOG_DELETE_OPERATION);
					}
					if (ctx.containsKey("document")) {
						data = (Map<String, Object>) ctx.get("document");
						logger.debug("From script document: {}", data);
					}
					if (ctx.containsKey("operation")) {
						operation = ctx.get("operation").toString();
						logger.debug("From script operation: {}", operation);
					}
				}
			}

			try {
				String index = extractIndex(ctx);
				String type = extractType(ctx);
				String parent = extractParent(ctx);
				String routing = extractRouting(ctx);
				objectId = extractObjectId(ctx, objectId);
				updateBulkRequest(bulk, data, objectId, operation, index, type,
						routing, parent);
			} catch (IOException e) {
				logger.warn("failed to parse {}", e, entry.getData());
			}
			return lastTimestamp;
		}

		private void updateBulkRequest(final BulkRequestBuilder bulk,
				Map<String, Object> data, String objectId, String operation,
				String index, String type, String routing, String parent)
				throws IOException {
			if (logger.isDebugEnabled()) {
				logger.debug(
						"Operation: {} - index: {} - type: {} - routing: {} - parent: {}",
						operation, index, type, routing, parent);
			}
			if (OPLOG_INSERT_OPERATION.equals(operation)) {
				if (logger.isDebugEnabled()) {
					logger.debug(
							"Insert operation - id: {} - contains attachment: {}",
							operation, objectId,
							data.containsKey(IS_MONGODB_ATTACHMENT));
				}
				bulk.add(indexRequest(index).type(type).id(objectId)
						.source(build(data, objectId)).routing(routing)
						.parent(parent));
				insertedDocuments++;
			}
			if (OPLOG_UPDATE_OPERATION.equals(operation)) {
				if (logger.isDebugEnabled()) {
					logger.debug(
							"Update operation - id: {} - contains attachment: {}",
							objectId, data.containsKey(IS_MONGODB_ATTACHMENT));
				}
				deleteBulkRequest(bulk, objectId, index, type, routing, parent);
				bulk.add(indexRequest(index).type(type).id(objectId)
						.source(build(data, objectId)).routing(routing)
						.parent(parent));
				updatedDocuments++;
			}
			if (OPLOG_DELETE_OPERATION.equals(operation)) {
				logger.info("Delete request [{}], [{}], [{}]", index, type,
						objectId);
				deleteBulkRequest(bulk, objectId, index, type, routing, parent);
				deletedDocuments++;
			}
			if (OPLOG_COMMAND_OPERATION.equals(operation)) {
				if (definition.isDropCollection()) {
					if (data.containsKey(OPLOG_DROP_COMMAND_OPERATION)
							&& data.get(OPLOG_DROP_COMMAND_OPERATION).equals(
									definition.getMongoCollection())) {
						logger.info("Drop collection request [{}], [{}]",
								index, type);
						bulk.request().requests().clear();
						client.admin().indices().prepareRefresh(index)
								.execute().actionGet();
						Map<String, MappingMetaData> mappings = client.admin()
								.cluster().prepareState().execute().actionGet()
								.getState().getMetaData().index(index)
								.mappings();
						logger.trace("mappings contains type {}: {}", type,
								mappings.containsKey(type));
						if (mappings.containsKey(type)) {
							/*
							 * Issue #105 - Mapping changing from custom mapping
							 * to dynamic when drop_collection = true Should
							 * capture the existing mapping metadata (in case it
							 * is has been customized before to delete.
							 */
							MappingMetaData mapping = mappings.get(type);
							client.admin().indices()
									.prepareDeleteMapping(index).setType(type)
									.execute().actionGet();
							PutMappingResponse pmr = client.admin().indices()
									.preparePutMapping(index).setType(type)
									.setSource(mapping.source().string())
									.execute().actionGet();
							if (!pmr.isAcknowledged()) {
								logger.error(
										"Failed to put mapping {} / {} / {}.",
										index, type, mapping.source());
							}
						}

						deletedDocuments = 0;
						updatedDocuments = 0;
						insertedDocuments = 0;
						logger.info(
								"Delete request for index / type [{}] [{}] successfully executed.",
								index, type);
					} else {
						logger.debug("Database command {}", data);
					}
				} else {
					logger.info(
							"Ignore drop collection request [{}], [{}]. The option has been disabled.",
							index, type);
				}
			}

		}

		/*
		 * Delete children when parent / child is used
		 */
		private void deleteBulkRequest(BulkRequestBuilder bulk,
				String objectId, String index, String type, String routing,
				String parent) {
			logger.trace(
					"bulkDeleteRequest - objectId: {} - index: {} - type: {} - routing: {} - parent: {}",
					objectId, index, type, routing, parent);

			if (definition.getParentTypes() != null
					&& definition.getParentTypes().contains(type)) {
				QueryBuilder builder = QueryBuilders.hasParentQuery(type,
						QueryBuilders.termQuery(MONGODB_ID_FIELD, objectId));
				SearchResponse response = client.prepareSearch(index)
						.setQuery(builder).setRouting(routing)
						.addField(MONGODB_ID_FIELD).execute().actionGet();
				for (SearchHit hit : response.getHits().getHits()) {
					bulk.add(deleteRequest(index).type(hit.getType())
							.id(hit.getId()).routing(routing).parent(objectId));
				}
			}
			bulk.add(deleteRequest(index).type(type).id(objectId)
					.routing(routing).parent(parent));
		}

		@SuppressWarnings("unchecked")
		private BSONTimestamp applyAdvancedTransformation(
				final BulkRequestBuilder bulk, QueueEntry entry) {

			BSONTimestamp lastTimestamp = entry.getOplogTimestamp();
			String operation = entry.getOperation();
			String objectId = "";
			if (entry.getData().get(MONGODB_ID_FIELD) != null) {
				objectId = entry.getData().get(MONGODB_ID_FIELD).toString();
			}
			if (logger.isDebugEnabled()) {
				logger.debug(
						"advancedUpdateBulkRequest for id: [{}], operation: [{}]",
						objectId, operation);
			}

			if (!definition.getIncludeCollection().isEmpty()) {
				logger.trace(
						"About to include collection. set attribute {} / {} ",
						definition.getIncludeCollection(),
						definition.getMongoCollection());
				entry.getData().put(definition.getIncludeCollection(),
						definition.getMongoCollection());
			}
			Map<String, Object> ctx = null;
			try {
				ctx = XContentFactory.xContent(XContentType.JSON)
						.createParser("{}").mapAndClose();
			} catch (Exception e) {
			}

			List<Object> documents = new ArrayList<Object>();
			Map<String, Object> document = new HashMap<String, Object>();

			if (scriptExecutable != null) {
				if (ctx != null && documents != null) {

					document.put("data", entry.getData());
					if (!objectId.isEmpty()) {
						document.put("id", objectId);
					}
					document.put("_index", definition.getIndexName());
					document.put("_type", definition.getTypeName());
					document.put("operation", operation);

					documents.add(document);

					ctx.put("documents", documents);
					if (logger.isDebugEnabled()) {
						logger.debug("Script to be executed: {}",
								scriptExecutable);
						logger.debug("Context before script executed: {}", ctx);
					}
					scriptExecutable.setNextVar("ctx", ctx);
					try {
						scriptExecutable.run();
						// we need to unwrap the context object...
						ctx = (Map<String, Object>) scriptExecutable
								.unwrap(ctx);
					} catch (Exception e) {
						logger.warn("failed to script process {}, ignoring", e,
								ctx);
					}
					if (logger.isDebugEnabled()) {
						logger.debug("Context after script executed: {}", ctx);
					}
					if (ctx.containsKey("documents")
							&& ctx.get("documents") instanceof List<?>) {
						documents = (List<Object>) ctx.get("documents");
						for (Object object : documents) {
							if (object instanceof Map<?, ?>) {
								Map<String, Object> item = (Map<String, Object>) object;
								logger.trace("item: {}", item);
								if (item.containsKey("deleted")
										&& item.get("deleted").equals(
												Boolean.TRUE)) {
									item.put("operation",
											OPLOG_DELETE_OPERATION);
								}

								String index = extractIndex(item);
								String type = extractType(item);
								String parent = extractParent(item);
								String routing = extractRouting(item);
								String action = extractOperation(item);
								boolean ignore = isDocumentIgnored(item);
								Map<String, Object> _data = (Map<String, Object>) item
										.get("data");
								objectId = extractObjectId(_data, objectId);
								if (logger.isDebugEnabled()) {
									logger.debug(
											"Id: {} - operation: {} - ignore: {} - index: {} - type: {} - routing: {} - parent: {}",
											objectId, action, ignore, index,
											type, routing, parent);
								}
								if (ignore) {
									continue;
								}
								try {
									updateBulkRequest(bulk, _data, objectId,
											operation, index, type, routing,
											parent);
								} catch (IOException ioEx) {
									logger.error("Update bulk failed.", ioEx);
								}
							}
						}
					}
				}
			}

			return lastTimestamp;
		}

		private XContentBuilder build(final Map<String, Object> data,
				final String objectId) throws IOException {
			if (data.containsKey(IS_MONGODB_ATTACHMENT)) {
				logger.info("Add Attachment: {} to index {} / type {}",
						objectId, definition.getIndexName(),
						definition.getTypeName());
				return MongoDBHelper.serialize((GridFSDBFile) data
						.get(MONGODB_ATTACHMENT));
			} else {
				return XContentFactory.jsonBuilder().map(data);
			}
		}

		private String extractObjectId(Map<String, Object> ctx, String objectId) {
			Object id = ctx.get("id");
			if (id != null) {
				return id.toString();
			}
			id = ctx.get(MONGODB_ID_FIELD);
			if (id != null) {
				return id.toString();
			} else {
				return objectId;
			}
		}

		private String extractParent(Map<String, Object> ctx) {
			Object parent = ctx.get("_parent");
			if (parent == null) {
				return null;
			} else {
				return parent.toString();
			}
		}

		private String extractRouting(Map<String, Object> ctx) {
			Object routing = ctx.get("_routing");
			if (routing == null) {
				return null;
			} else {
				return routing.toString();
			}
		}

		private String extractOperation(Map<String, Object> ctx) {
			Object operation = ctx.get("operation");
			if (operation == null) {
				return null;
			} else {
				return operation.toString();
			}
		}

		private boolean isDocumentIgnored(Map<String, Object> ctx) {
			return (ctx.containsKey("ignore") && ctx.get("ignore").equals(
					Boolean.TRUE));
		}

		private String extractType(Map<String, Object> ctx) {
			Object type = ctx.get("_type");
			if (type == null) {
				return definition.getTypeName();
			} else {
				return type.toString();
			}
		}

		private String extractIndex(Map<String, Object> ctx) {
			String index = (String) ctx.get("_index");
			if (index == null) {
				index = definition.getIndexName();
			}
			return index;
		}

		private void logStatistics() {
			long totalDocuments = deletedDocuments + insertedDocuments;
			long totalTimeInSeconds = sw.stop().totalTime().seconds();
			long totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? totalDocuments
					: totalDocuments / totalTimeInSeconds;
			logger.info(
					"Indexed {} documents, {} insertions, {} updates, {} deletions, {} documents per second",
					totalDocuments, insertedDocuments, updatedDocuments,
					deletedDocuments, totalDocumentsPerSecond);
		}
	}

	private class Slurper implements Runnable {

		private Mongo mongo;
		private DB slurpedDb;
		private DBCollection slurpedCollection;
		private DB oplogDb;
		private DBCollection oplogCollection;

		public Slurper(List<ServerAddress> mongoServers) {
			this.mongo = new MongoClient(mongoServers,
				definition.getMongoClientOptions());
		}

		@Override
		public void run() {
			while (active) {
				try {
					if (!assignCollections()) {
						break;	// failed to assign oplogCollection or
								// slurpedCollection
					}

					DBCursor cursor = null;

					BSONTimestamp startTimestamp = null;

					// Do an initial sync the same way MongoDB does
					// https://groups.google.com/forum/?fromgroups=#!topic/mongodb-user/sOKlhD_E2ns
					// TODO: support gridfs
					if (!definition.isMongoGridFS()) {
						BSONTimestamp lastIndexedTimestamp = getLastTimestamp(mongoOplogNamespace);
						if (lastIndexedTimestamp == null) {
							// TODO: ensure the index type is empty
							logger.info("MongoDBRiver is beginning initial import of "
								+ slurpedCollection.getFullName());
							startTimestamp = getCurrentOplogTimestamp();
							try {
								cursor = slurpedCollection.find();
								while (cursor.hasNext()) {
									DBObject object = cursor.next();
									Map<String, Object> map = applyFieldFilter(object).toMap();
									addToStream(OPLOG_INSERT_OPERATION, null, map);
								}	
							} finally {
								if (cursor != null) {
									logger.trace("Closing initial import cursor");
									cursor.close();
								}
							}
						}
					}

					// Slurp from oplog
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
				} catch (MongoException mEx) {
					logger.error("Mongo gave an exception", mEx);
				} catch (NoSuchElementException nEx) {
					logger.warn("A mongoDB cursor bug ?", nEx);
				} catch (InterruptedException e) {
					if (logger.isDebugEnabled()) {
						logger.debug("river-mongodb slurper interrupted");
					}
					Thread.currentThread().interrupt();
					break;
				}
			}
		}

		private boolean assignCollections() {
			DB adminDb = mongo.getDB(MONGODB_ADMIN_DATABASE);
			oplogDb = mongo.getDB(MONGODB_LOCAL_DATABASE);

			if (!definition.getMongoAdminUser().isEmpty()
					&& !definition.getMongoAdminPassword().isEmpty()) {
				logger.info("Authenticate {} with {}", MONGODB_ADMIN_DATABASE,
						definition.getMongoAdminUser());

				CommandResult cmd = adminDb.authenticateCommand(definition
						.getMongoAdminUser(), definition
						.getMongoAdminPassword().toCharArray());
				if (!cmd.ok()) {
					logger.error("Autenticatication failed for {}: {}",
							MONGODB_ADMIN_DATABASE, cmd.getErrorMessage());
					// Can still try with mongoLocal credential if provided.
					// return false;
				}
				oplogDb = adminDb.getMongo().getDB(MONGODB_LOCAL_DATABASE);
			}

			if (!definition.getMongoLocalUser().isEmpty()
					&& !definition.getMongoLocalPassword().isEmpty()
					&& !oplogDb.isAuthenticated()) {
				logger.info("Authenticate {} with {}", MONGODB_LOCAL_DATABASE,
						definition.getMongoLocalUser());
				CommandResult cmd = oplogDb.authenticateCommand(definition
						.getMongoLocalUser(), definition
						.getMongoLocalPassword().toCharArray());
				if (!cmd.ok()) {
					logger.error("Autenticatication failed for {}: {}",
							MONGODB_LOCAL_DATABASE, cmd.getErrorMessage());
					return false;
				}
			}

			Set<String> collections = oplogDb.getCollectionNames();
			if (!collections.contains(OPLOG_COLLECTION)) {
				logger.error("Cannot find "
						+ OPLOG_COLLECTION
						+ " collection. Please check this link: http://goo.gl/2x5IW");
				return false;
			}
			oplogCollection = oplogDb.getCollection(OPLOG_COLLECTION);

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
					.sort(new BasicDBObject(OPLOG_TIMESTAMP, -1))
					.limit(1)
					.next()
					.get(OPLOG_TIMESTAMP);
		}

		private DBCursor processFullOplog() throws InterruptedException {
			BSONTimestamp currentTimestamp = getCurrentOplogTimestamp();
			addQueryToStream(OPLOG_INSERT_OPERATION, currentTimestamp, null);
			return oplogCursor(currentTimestamp);
		}

		@SuppressWarnings("unchecked")
		private void processOplogEntry(final DBObject entry)
				throws InterruptedException {
			String operation = entry.get(OPLOG_OPERATION).toString();
			String namespace = entry.get(OPLOG_NAMESPACE).toString();
			BSONTimestamp oplogTimestamp = (BSONTimestamp) entry
					.get(OPLOG_TIMESTAMP);
			DBObject object = (DBObject) entry.get(OPLOG_OBJECT);

			if (logger.isTraceEnabled()) {
				logger.trace("MongoDB object deserialized: {}",
						object.toString());
			}

			// Initial support for sharded collection -
			// https://jira.mongodb.org/browse/SERVER-4333
			// Not interested in operation from migration or sharding
			if (entry.containsField(OPLOG_FROM_MIGRATE)
					&& ((BasicBSONObject) entry).getBoolean(OPLOG_FROM_MIGRATE)) {
				logger.debug(
						"From migration or sharding operation. Can be ignored. {}",
						entry);
				return;
			}
			// Not interested by chunks - skip all
			if (namespace.endsWith(GRIDFS_CHUNKS_SUFFIX)) {
				return;
			}

			if (logger.isTraceEnabled()) {
				logger.trace("oplog entry - namespace [{}], operation [{}]",
						namespace, operation);
				logger.trace("oplog processing item {}", entry);
			}

			String objectId = getObjectIdFromOplogEntry(entry);
			if (definition.isMongoGridFS()
					&& namespace.endsWith(GRIDFS_FILES_SUFFIX)
					&& (OPLOG_INSERT_OPERATION.equals(operation) || OPLOG_UPDATE_OPERATION
							.equals(operation))) {
				if (objectId == null) {
					throw new NullPointerException(MONGODB_ID_FIELD);
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
					throw new NullPointerException(MONGODB_ID_FIELD);
				}
				logger.info("Add attachment: {}", objectId);
				object = applyFieldFilter(object);
				HashMap<String, Object> data = new HashMap<String, Object>();
				data.put(IS_MONGODB_ATTACHMENT, true);
				data.put(MONGODB_ATTACHMENT, object);
				data.put(MONGODB_ID_FIELD, objectId);
				addToStream(operation, oplogTimestamp, data);
			} else {
				if (OPLOG_UPDATE_OPERATION.equals(operation)) {
					DBObject update = (DBObject) entry.get(OPLOG_UPDATE);
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
			if (entry.containsField(OPLOG_OBJECT)) {
				DBObject object = (DBObject) entry.get(OPLOG_OBJECT);
				if (object.containsField(MONGODB_ID_FIELD)) {
					return object.get(MONGODB_ID_FIELD).toString();
				}
			}
			if (entry.containsField(OPLOG_UPDATE)) {
				DBObject object = (DBObject) entry.get(OPLOG_UPDATE);
				if (object.containsField(MONGODB_ID_FIELD)) {
					return object.get(MONGODB_ID_FIELD).toString();
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
				filter.put(OPLOG_TIMESTAMP, new BasicDBObject(
						QueryOperators.GT, time));
			}

			if (definition.isMongoGridFS()) {
				filter.put(OPLOG_NAMESPACE, mongoOplogNamespace
						+ GRIDFS_FILES_SUFFIX);
			} else {
				values2.add(new BasicDBObject(OPLOG_NAMESPACE,
						mongoOplogNamespace));
				values2.add(new BasicDBObject(OPLOG_NAMESPACE, definition
						.getMongoDb() + "." + OPLOG_NAMESPACE_COMMAND));
				filter.put(MONGODB_OR_OPERATOR, values2);
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
			filters.add(new BasicDBObject(OPLOG_OPERATION,
					OPLOG_DELETE_OPERATION));

			// include update, insert in filters3
			filters3.add(new BasicDBObject(OPLOG_OPERATION,
					OPLOG_UPDATE_OPERATION));
			filters3.add(new BasicDBObject(OPLOG_OPERATION,
					OPLOG_INSERT_OPERATION));

			// include or operation statement in filter2
			filters2.add(new BasicDBObject(MONGODB_OR_OPERATOR, filters3));

			// include custom filter in filters2
			filters2.add((DBObject) JSON.parse(definition.getMongoFilter()));

			filters.add(new BasicDBObject(MONGODB_AND_OPERATOR, filters2));

			return new BasicDBObject(MONGODB_OR_OPERATOR, filters);
		}

		private DBCursor oplogCursor(final BSONTimestamp timestampOverride) {
			BSONTimestamp time = timestampOverride == null
					? getLastTimestamp(mongoOplogNamespace) : timestampOverride;
			DBObject indexFilter = getOplogFilter(time);
			if (indexFilter == null) {
				return null;
			}

			int options = Bytes.QUERYOPTION_TAILABLE
					| Bytes.QUERYOPTION_AWAITDATA | Bytes.QUERYOPTION_NOTIMEOUT;

			// Using OPLOGREPLAY to improve performance:
			// https://jira.mongodb.org/browse/JAVA-771
			if (indexFilter.containsField(OPLOG_TIMESTAMP)) {
				options = options | Bytes.QUERYOPTION_OPLOGREPLAY;
			}
			return oplogCollection.find(indexFilter)
					.sort(new BasicDBObject(MONGODB_NATURAL_OPERATOR, 1))
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

			stream.put(new QueueEntry(currentTimestamp, operation, data));
		}

	}

	private class Status implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					if (startInvoked) {
						boolean enabled = MongoDBRiverHelper.isRiverEnabled(
								client, riverName.getName());

						if (active && !enabled) {
							logger.info("About to stop river: {}",
									riverName.getName());
							close();
						}

						if (!active && enabled) {
							logger.trace("About to start river: {}",
									riverName.getName());
							start();
						}
					}
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					logger.info("Status thread interrupted", e, (Object) null);
					Thread.currentThread().interrupt();
					break;
				}

			}
		}
	}

	private XContentBuilder getGridFSMapping() throws IOException {
		XContentBuilder mapping = jsonBuilder().startObject()
				.startObject(definition.getTypeName())
				.startObject("properties").startObject("content")
				.field("type", "attachment").endObject()
				.startObject("filename").field("type", "string").endObject()
				.startObject("contentType").field("type", "string").endObject()
				.startObject("md5").field("type", "string").endObject()
				.startObject("length").field("type", "long").endObject()
				.startObject("chunkSize").field("type", "long").endObject()
				.endObject().endObject().endObject();
		logger.info("Mapping: {}", mapping.string());
		return mapping;
	}

	/**
	 * Get the latest timestamp for a given namespace.
	 */
	@SuppressWarnings("unchecked")
	private BSONTimestamp getLastTimestamp(final String namespace) {
		GetResponse lastTimestampResponse = client
				.prepareGet(riverIndexName, riverName.getName(), namespace)
				.execute().actionGet();
		// API changes since 0.90.0 lastTimestampResponse.exists() replaced by
		// lastTimestampResponse.isExists()
		if (lastTimestampResponse.isExists()) {
			// API changes since 0.90.0 lastTimestampResponse.sourceAsMap()
			// replaced by lastTimestampResponse.getSourceAsMap()
			Map<String, Object> mongodbState = (Map<String, Object>) lastTimestampResponse
					.getSourceAsMap().get(TYPE);
			if (mongodbState != null) {
				String lastTimestamp = mongodbState.get(LAST_TIMESTAMP_FIELD)
						.toString();
				if (lastTimestamp != null) {
					if (logger.isDebugEnabled()) {
						logger.debug("{} last timestamp: {}", namespace,
								lastTimestamp);
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
	private void updateLastTimestamp(final String namespace,
			final BSONTimestamp time, final BulkRequestBuilder bulk) {
		try {
			bulk.add(indexRequest(riverIndexName)
					.type(riverName.getName())
					.id(namespace)
					.source(jsonBuilder().startObject().startObject(TYPE)
							.field(LAST_TIMESTAMP_FIELD, JSON.serialize(time))
							.endObject().endObject()));
		} catch (IOException e) {
			logger.error("error updating last timestamp for namespace {}",
					namespace);
		}
	}

	protected static class QueueEntry {

		private Map<String, Object> data;
		private String operation;
		private BSONTimestamp oplogTimestamp;

		public QueueEntry(
				Map<String, Object> data) {
			this.data = data;
			this.operation = OPLOG_INSERT_OPERATION;
		}

		public QueueEntry(
				BSONTimestamp oplogTimestamp,
				String oplogOperation,
				Map<String, Object> data) {
			this.data = data;
			this.operation = oplogOperation;
			this.oplogTimestamp = oplogTimestamp;
		}

		public boolean isOplogEntry() {
		  return oplogTimestamp != null;
		}

		public Map<String, Object> getData() {
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
