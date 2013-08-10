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
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.QueryOperators;
import com.mongodb.ReadPreference;
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
	public final static String DB_FIELD = "db";
	public final static String SERVERS_FIELD = "servers";
	public final static String HOST_FIELD = "host";
	public final static String PORT_FIELD = "port";
	public final static String OPTIONS_FIELD = "options";
	public final static String SECONDARY_READ_PREFERENCE_FIELD = "secondary_read_preference";
	public final static String SSL_CONNECTION_FIELD = "ssl";
	public final static String SSL_VERIFY_CERT_FIELD = "ssl_verify_certificate";
	public final static String DROP_COLLECTION_FIELD = "drop_collection";
	public final static String EXCLUDE_FIELDS_FIELD = "exclude_fields";
	public final static String INCLUDE_COLLECTION_FIELD = "include_collection";
	public final static String INITIAL_TIMESTAMP_FIELD = "initial_timestamp";
	public final static String INITIAL_TIMESTAMP_SCRIPT_TYPE_FIELD = "script_type";
	public final static String INITIAL_TIMESTAMP_SCRIPT_FIELD = "script";
	public final static String FILTER_FIELD = "filter";
	public final static String CREDENTIALS_FIELD = "credentials";
	public final static String USER_FIELD = "user";
	public final static String PASSWORD_FIELD = "password";
	public final static String SCRIPT_FIELD = "script";
	public final static String SCRIPT_TYPE_FIELD = "scriptType";
	public final static String COLLECTION_FIELD = "collection";
	public final static String GRIDFS_FIELD = "gridfs";
	public final static String INDEX_OBJECT = "index";
	public final static String NAME_FIELD = "name";
	public final static String TYPE_FIELD = "type";
	public final static String LOCAL_DB_FIELD = "local";
	public final static String ADMIN_DB_FIELD = "admin";
	public final static String DB_LOCAL = "local";
	public final static String DB_ADMIN = "admin";
	public final static String DB_CONFIG = "config";
	public final static String DEFAULT_DB_HOST = "localhost";
	public final static String THROTTLE_SIZE_FIELD = "throttle_size";
	public final static int DEFAULT_DB_PORT = 27017;
	public final static String BULK_SIZE_FIELD = "bulk_size";
	public final static String BULK_TIMEOUT_FIELD = "bulk_timeout";
	public final static String LAST_TIMESTAMP_FIELD = "_last_ts";
	public final static String MONGODB_LOCAL = "local";
	public final static String MONGODB_ADMIN = "admin";
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
	public final static String GRIDFS_FILES_SUFFIX = ".files";
	public final static String GRIDFS_CHUNKS_SUFFIX = ".chunks";

	protected final Client client;

	protected final String riverIndexName;

	protected final List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
	protected final String mongoDb;
	protected final String mongoCollection;
	protected final boolean mongoGridFS;
	protected final String mongoFilter;
	protected final String mongoAdminUser;
	protected final String mongoAdminPassword;
	protected final String mongoLocalUser;
	protected final String mongoLocalPassword;
	protected final String mongoOplogNamespace;
	protected final boolean mongoSecondaryReadPreference;
	protected final boolean mongoUseSSL;
	protected final boolean mongoSSLVerifyCertificate;

	protected final String script;
	protected final String scriptType;
	protected final String indexName;
	protected final String typeName;
	protected final int bulkSize;
	protected final TimeValue bulkTimeout;
	protected final int throttleSize;
	protected final boolean dropCollection;
	protected final Set<String> excludeFields;
	protected final String includeCollection;
	protected final BSONTimestamp initialTimestamp;

	protected final BasicDBObject findKeys = new BasicDBObject();
	protected final ScriptService scriptService;

	protected volatile List<Thread> tailerThreads = new ArrayList<Thread>();
	protected volatile Thread indexerThread;
	protected volatile Thread statusThread;
	protected volatile boolean active = false;
	protected volatile boolean startInvoked = false;

	// private final TransferQueue<Map<String, Object>> stream = new
	// LinkedTransferQueue<Map<String, Object>>();
	private final BlockingQueue<Map<String, Object>> stream;
	private SocketFactory sslSocketFactory;

	private Mongo mongo;
	private DB adminDb;

	@SuppressWarnings("unchecked")
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
		String mongoHost;
		int mongoPort;

		if (settings.settings().containsKey(TYPE)) {
			Map<String, Object> mongoSettings = (Map<String, Object>) settings
					.settings().get(TYPE);
			if (mongoSettings.containsKey(SERVERS_FIELD)) {
				Object mongoServersSettings = mongoSettings.get(SERVERS_FIELD);
				logger.info("mongoServersSettings: " + mongoServersSettings);
				boolean array = XContentMapValues.isArray(mongoServersSettings);

				if (array) {
					ArrayList<Map<String, Object>> feeds = (ArrayList<Map<String, Object>>) mongoServersSettings;
					for (Map<String, Object> feed : feeds) {
						mongoHost = XContentMapValues.nodeStringValue(
								feed.get(HOST_FIELD), null);
						mongoPort = XContentMapValues.nodeIntegerValue(
								feed.get(PORT_FIELD), 0);
						logger.info("Server: " + mongoHost + " - " + mongoPort);
						try {
							mongoServers.add(new ServerAddress(mongoHost,
									mongoPort));
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else {
				mongoHost = XContentMapValues.nodeStringValue(
						mongoSettings.get(HOST_FIELD), DEFAULT_DB_HOST);
				mongoPort = XContentMapValues.nodeIntegerValue(
						mongoSettings.get(PORT_FIELD), DEFAULT_DB_PORT);
				try {
					mongoServers.add(new ServerAddress(mongoHost, mongoPort));
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// MongoDB options
			if (mongoSettings.containsKey(OPTIONS_FIELD)) {
				Map<String, Object> mongoOptionsSettings = (Map<String, Object>) mongoSettings
						.get(OPTIONS_FIELD);
				logger.trace("mongoOptionsSettings: " + mongoOptionsSettings);
				mongoSecondaryReadPreference = XContentMapValues
						.nodeBooleanValue(mongoOptionsSettings
								.get(SECONDARY_READ_PREFERENCE_FIELD), false);
				dropCollection = XContentMapValues.nodeBooleanValue(
						mongoOptionsSettings.get(DROP_COLLECTION_FIELD), false);
				mongoUseSSL = XContentMapValues.nodeBooleanValue(
						mongoOptionsSettings.get(SSL_CONNECTION_FIELD), false);
				mongoSSLVerifyCertificate = XContentMapValues.nodeBooleanValue(
						mongoOptionsSettings.get(SSL_VERIFY_CERT_FIELD), true);

				includeCollection = XContentMapValues.nodeStringValue(
						mongoOptionsSettings.get(INCLUDE_COLLECTION_FIELD), "");
				if (mongoOptionsSettings.containsKey(EXCLUDE_FIELDS_FIELD)) {
					excludeFields = new HashSet<String>();
					Object excludeFieldsSettings = mongoOptionsSettings
							.get(EXCLUDE_FIELDS_FIELD);
					logger.info("excludeFieldsSettings: "
							+ excludeFieldsSettings);
					boolean array = XContentMapValues
							.isArray(excludeFieldsSettings);

					if (array) {
						ArrayList<String> fields = (ArrayList<String>) excludeFieldsSettings;
						for (String field : fields) {
							logger.info("Field: " + field);
							excludeFields.add(field);
						}
					}

					if (excludeFields != null) {
						for (String key : excludeFields) {
							findKeys.put(key, 0);
						}
					}
				} else {
					excludeFields = null;
				}
				if (mongoOptionsSettings.containsKey(INITIAL_TIMESTAMP_FIELD)) {
					BSONTimestamp timeStamp = null;
					try {
						Map<String, Object> initalTimestampSettings = (Map<String, Object>) mongoOptionsSettings
								.get(INITIAL_TIMESTAMP_FIELD);
						String scriptType = "js";
						if (initalTimestampSettings
								.containsKey(INITIAL_TIMESTAMP_SCRIPT_TYPE_FIELD)) {
							scriptType = initalTimestampSettings.get(
									INITIAL_TIMESTAMP_SCRIPT_TYPE_FIELD)
									.toString();
						}
						if (initalTimestampSettings
								.containsKey(INITIAL_TIMESTAMP_SCRIPT_FIELD)) {

							ExecutableScript scriptExecutable = scriptService
									.executable(
											scriptType,
											initalTimestampSettings
													.get(INITIAL_TIMESTAMP_SCRIPT_FIELD)
													.toString(), Maps
													.newHashMap());
							Object ctx = scriptExecutable.run();
							logger.trace(
									"initialTimestamp script returned: {}", ctx);
							if (ctx != null) {
								long timestamp = Long.parseLong(ctx.toString());
								timeStamp = new BSONTimestamp((int) (new Date(
										timestamp).getTime() / 1000), 1);
							}
						}
					} catch (Throwable t) {
						logger.warn("Could set initial timestamp", t,
								new Object());
					} finally {
						initialTimestamp = timeStamp;
					}
				} else {
					initialTimestamp = null;
				}
			} else {
				mongoSecondaryReadPreference = false;
				dropCollection = false;
				includeCollection = "";
				excludeFields = null;
				mongoUseSSL = false;
				mongoSSLVerifyCertificate = false;
				initialTimestamp = null;
			}

			// Credentials
			if (mongoSettings.containsKey(CREDENTIALS_FIELD)) {
				String dbCredential;
				String mau = "";
				String map = "";
				String mlu = "";
				String mlp = "";
				// String mdu = "";
				// String mdp = "";
				Object mongoCredentialsSettings = mongoSettings
						.get(CREDENTIALS_FIELD);
				boolean array = XContentMapValues
						.isArray(mongoCredentialsSettings);

				if (array) {
					ArrayList<Map<String, Object>> credentials = (ArrayList<Map<String, Object>>) mongoCredentialsSettings;
					for (Map<String, Object> credential : credentials) {
						dbCredential = XContentMapValues.nodeStringValue(
								credential.get(DB_FIELD), null);
						if (ADMIN_DB_FIELD.equals(dbCredential)) {
							mau = XContentMapValues.nodeStringValue(
									credential.get(USER_FIELD), null);
							map = XContentMapValues.nodeStringValue(
									credential.get(PASSWORD_FIELD), null);
						} else if (LOCAL_DB_FIELD.equals(dbCredential)) {
							mlu = XContentMapValues.nodeStringValue(
									credential.get(USER_FIELD), null);
							mlp = XContentMapValues.nodeStringValue(
									credential.get(PASSWORD_FIELD), null);
							// } else {
							// mdu = XContentMapValues.nodeStringValue(
							// credential.get(USER_FIELD), null);
							// mdp = XContentMapValues.nodeStringValue(
							// credential.get(PASSWORD_FIELD), null);
						}
					}
				}
				mongoAdminUser = mau;
				mongoAdminPassword = map;
				mongoLocalUser = mlu;
				mongoLocalPassword = mlp;
				// mongoDbUser = mdu;
				// mongoDbPassword = mdp;

			} else {
				mongoAdminUser = "";
				mongoAdminPassword = "";
				mongoLocalUser = "";
				mongoLocalPassword = "";
				// mongoDbUser = "";
				// mongoDbPassword = "";
			}

			mongoDb = XContentMapValues.nodeStringValue(
					mongoSettings.get(DB_FIELD), riverName.name());
			mongoCollection = XContentMapValues.nodeStringValue(
					mongoSettings.get(COLLECTION_FIELD), riverName.name());
			mongoGridFS = XContentMapValues.nodeBooleanValue(
					mongoSettings.get(GRIDFS_FIELD), false);
			if (mongoSettings.containsKey(FILTER_FIELD)) {
				mongoFilter = XContentMapValues.nodeStringValue(
						mongoSettings.get(FILTER_FIELD), "");
			} else {
				mongoFilter = "";
			}

			if (mongoSettings.containsKey(SCRIPT_FIELD)) {
				// String scriptType = "js";
				script = mongoSettings.get(SCRIPT_FIELD).toString();
				if (mongoSettings.containsKey(SCRIPT_TYPE_FIELD)) {
					// scriptType = mongoSettings.get(SCRIPT_TYPE_FIELD)
					// .toString();
					scriptType = mongoSettings.get(SCRIPT_TYPE_FIELD)
							.toString();
				} else {
					scriptType = "js";
				}

				// script = scriptService.executable(scriptType, myScript,
				// ImmutableMap.of(
				// "logger", logger));
			} else {
				script = null;
				scriptType = null;
			}
		} else {
			mongoHost = DEFAULT_DB_HOST;
			mongoPort = DEFAULT_DB_PORT;
			try {
				mongoServers.add(new ServerAddress(mongoHost, mongoPort));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			mongoSecondaryReadPreference = false;
			mongoDb = riverName.name();
			mongoCollection = riverName.name();
			mongoFilter = "";
			mongoGridFS = false;
			mongoAdminUser = "";
			mongoAdminPassword = "";
			mongoLocalUser = "";
			mongoLocalPassword = "";
			// mongoDbUser = "";
			// mongoDbPassword = "";
			// script = null;
			script = null;
			scriptType = null;
			dropCollection = false;
			includeCollection = "";
			excludeFields = null;
			mongoUseSSL = false;
			mongoSSLVerifyCertificate = false;
			initialTimestamp = null;
		}
		mongoOplogNamespace = mongoDb + "." + mongoCollection;

		if (settings.settings().containsKey(INDEX_OBJECT)) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings
					.settings().get(INDEX_OBJECT);
			indexName = XContentMapValues.nodeStringValue(
					indexSettings.get(NAME_FIELD), mongoDb);
			typeName = XContentMapValues.nodeStringValue(
					indexSettings.get(TYPE_FIELD), mongoDb);
			bulkSize = XContentMapValues.nodeIntegerValue(
					indexSettings.get(BULK_SIZE_FIELD), 100);
			if (indexSettings.containsKey(BULK_TIMEOUT_FIELD)) {
				bulkTimeout = TimeValue.parseTimeValue(
						XContentMapValues.nodeStringValue(
								indexSettings.get(BULK_TIMEOUT_FIELD), "10ms"),
						TimeValue.timeValueMillis(10));
			} else {
				bulkTimeout = TimeValue.timeValueMillis(10);
			}
			throttleSize = XContentMapValues.nodeIntegerValue(
					indexSettings.get(THROTTLE_SIZE_FIELD), bulkSize * 5);
		} else {
			indexName = mongoDb;
			typeName = mongoDb;
			bulkSize = 100;
			bulkTimeout = TimeValue.timeValueMillis(10);
			throttleSize = bulkSize * 5;
		}
		if (throttleSize == -1) {
			stream = new LinkedTransferQueue<Map<String, Object>>();
		} else {
			stream = new ArrayBlockingQueue<Map<String, Object>>(throttleSize);
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
		for (ServerAddress server : mongoServers) {
			logger.info("Using mongodb server(s): host [{}], port [{}]",
					server.getHost(), server.getPort());
		}
		// TODO: include plugin version from pom.properties file.
		// http://stackoverflow.com/questions/5270611/read-maven-properties-file-inside-jar-war-file
		logger.info(
				"starting mongodb stream. options: secondaryreadpreference [{}], drop_collection [{}], include_collection [{}], throttlesize [{}], gridfs [{}], filter [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]",
				mongoSecondaryReadPreference, dropCollection,
				includeCollection, throttleSize, mongoGridFS, mongoFilter,
				mongoDb, mongoCollection, script, indexName, typeName);
		try {
			client.admin().indices().prepareCreate(indexName).execute()
					.actionGet();
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
						e, indexName);
				return;
			}
		}

		if (mongoGridFS) {
			try {
				if (logger.isDebugEnabled()) {
					logger.debug("Set explicit attachment mapping.");
				}
				client.admin().indices().preparePutMapping(indexName)
						.setType(typeName).setSource(getGridFSMapping())
						.execute().actionGet();
			} catch (Exception e) {
				logger.warn("Failed to set explicit mapping (attachment): {}",
						e);
			}
		}

		if (isMongos()) {
			DBCursor cursor = getConfigDb().getCollection("shards").find();
			while (cursor.hasNext()) {
				DBObject item = cursor.next();
				logger.info(item.toString());
				List<ServerAddress> servers = getServerAddressForReplica(item);
				if (servers != null) {
					String replicaName = item.get("_id").toString();
					Thread tailerThread = EsExecutors.daemonThreadFactory(
							settings.globalSettings(),
							"mongodb_river_slurper-" + replicaName).newThread(
							new Slurper(servers));
					tailerThreads.add(tailerThread);
				}
			}
		} else {
			Thread tailerThread = EsExecutors.daemonThreadFactory(
					settings.globalSettings(), "mongodb_river_slurper")
					.newThread(new Slurper(mongoServers));
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
		// statusThread = EsExecutors.daemonThreadFactory(
		// settings.globalSettings(), "mongodb_river_status").newThread(
		// new Status());
		// statusThread.start();
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
		// return (cr.get("process").equals("mongos"));
		// Fix for https://jira.mongodb.org/browse/SERVER-9160
		return (process.contains("mongos"));
	}

	private DB getAdminDb() {
		if (adminDb == null) {
			adminDb = getMongoClient().getDB(DB_ADMIN);
			if (!mongoAdminUser.isEmpty() && !mongoAdminPassword.isEmpty()
					&& !adminDb.isAuthenticated()) {
				logger.info("Authenticate {} with {}", MONGODB_ADMIN,
						mongoAdminUser);

				try {
					CommandResult cmd = adminDb.authenticateCommand(
							mongoAdminUser, mongoAdminPassword.toCharArray());
					if (!cmd.ok()) {
						logger.error("Autenticatication failed for {}: {}",
								MONGODB_ADMIN, cmd.getErrorMessage());
					}
				} catch (MongoException mEx) {
					logger.warn("getAdminDb() failed", mEx);
				}
			}
		}
		return adminDb;
	}

	private DB getConfigDb() {
		DB configDb = getMongoClient().getDB(DB_CONFIG);
		if (!mongoAdminUser.isEmpty() && !mongoAdminUser.isEmpty()
				&& getAdminDb().isAuthenticated()) {
			configDb = getAdminDb().getMongo().getDB(DB_CONFIG);
			// } else if (!mongoDbUser.isEmpty() && !mongoDbPassword.isEmpty()
			// && !configDb.isAuthenticated()) {
			// logger.info("Authenticate {} with {}", mongoDb, mongoDbUser);
			// CommandResult cmd = configDb.authenticateCommand(mongoDbUser,
			// mongoDbPassword.toCharArray());
			// if (!cmd.ok()) {
			// logger.error("Autenticatication failed for {}: {}",
			// DB_CONFIG, cmd.getErrorMessage());
			// }
		}
		return configDb;
	}

	// TODO: MongoClientOptions should be configurable
	private Mongo getMongoClient() {
		if (mongo == null) {
			Builder builder = MongoClientOptions.builder()
					.autoConnectRetry(true).connectTimeout(15000)
					.socketKeepAlive(true).socketTimeout(60000);
			if (mongoUseSSL) {
				builder.socketFactory(getSSLSocketFactory());
			}

			MongoClientOptions mco = builder.build();
			mongo = new MongoClient(mongoServers, mco);
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
		// if (active) {
		logger.info("closing mongodb stream river");
		try {
			// active = false;
			for (Thread thread : tailerThreads) {
				thread.interrupt();
			}
			tailerThreads.clear();
			if (indexerThread != null) {
				indexerThread.interrupt();
			}
			// if (statusThread != null) {
			// statusThread.interrupt();
			// }
			// }
			closeMongoClient();
		} catch (Throwable t) {
			logger.error("Fail to close river {}", t, riverName.getName());
		} finally {
			active = false;
		}
	}

	private SocketFactory getSSLSocketFactory() {
		if (sslSocketFactory != null)
			return sslSocketFactory;

		if (!mongoSSLVerifyCertificate) {
			try {
				final TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {

					@Override
					public X509Certificate[] getAcceptedIssuers() {
						return null;
					}

					@Override
					public void checkServerTrusted(X509Certificate[] chain,
							String authType) throws CertificateException {
					}

					@Override
					public void checkClientTrusted(X509Certificate[] chain,
							String authType) throws CertificateException {
					}
				} };
				final SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts,
						new java.security.SecureRandom());
				// Create an ssl socket factory with our all-trusting manager
				sslSocketFactory = sslContext.getSocketFactory();
				return sslSocketFactory;
			} catch (Exception ex) {
				logger.error(
						"Unable to build ssl socket factory without certificate validation, using default instead.",
						ex);
			}
		}
		sslSocketFactory = SSLSocketFactory.getDefault();
		return sslSocketFactory;
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

				if (script != null && scriptType != null) {
					scriptExecutable = scriptService.executable(scriptType,
							script, ImmutableMap.of("logger", logger));
				}

				try {
					BSONTimestamp lastTimestamp = null;
					BulkRequestBuilder bulk = client.prepareBulk();

					// 1. Attempt to fill as much of the bulk request as
					// possible
					Map<String, Object> data = stream.take();
					lastTimestamp = updateBulkRequest(bulk, data);
					while ((data = stream.poll(bulkTimeout.millis(),
							MILLISECONDS)) != null) {
						lastTimestamp = updateBulkRequest(bulk, data);
						if (bulk.numberOfActions() >= bulkSize) {
							break;
						}
					}

					// 2. Update the timestamp
					if (lastTimestamp != null) {
						updateLastTimestamp(mongoOplogNamespace, lastTimestamp,
								bulk);
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
		private BSONTimestamp updateBulkRequest(final BulkRequestBuilder bulk,
				Map<String, Object> data) {
			if (data.get(MONGODB_ID_FIELD) == null
					&& !data.get(OPLOG_OPERATION).equals(
							OPLOG_COMMAND_OPERATION)) {
				logger.warn(
						"Cannot get object id. Skip the current item: [{}]",
						data);
				return null;
			}
			BSONTimestamp lastTimestamp = (BSONTimestamp) data
					.get(OPLOG_TIMESTAMP);
			String operation = data.get(OPLOG_OPERATION).toString();
			// String objectId = data.get(MONGODB_ID_FIELD).toString();
			String objectId = "";
			if (data.get(MONGODB_ID_FIELD) != null) {
				objectId = data.get(MONGODB_ID_FIELD).toString();
			}
			data.remove(OPLOG_TIMESTAMP);
			data.remove(OPLOG_OPERATION);
			if (logger.isDebugEnabled()) {
				logger.debug("updateBulkRequest for id: [{}], operation: [{}]",
						objectId, operation);
			}

			if (!includeCollection.isEmpty()) {
				logger.trace(
						"About to include collection. set attribute {} / {} ",
						includeCollection, mongoCollection);
				data.put(includeCollection, mongoCollection);
			}

			Map<String, Object> ctx = null;
			try {
				ctx = XContentFactory.xContent(XContentType.JSON)
						.createParser("{}").mapAndClose();
			} catch (IOException e) {
				logger.warn("failed to parse {}", e);
			}
			if (scriptExecutable != null) {
				if (ctx != null) {
					ctx.put("document", data);
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
								objectId,
								data.containsKey(IS_MONGODB_ATTACHMENT));
					}
					bulk.add(new DeleteRequest(index, type, objectId).routing(
							routing).parent(parent));
					bulk.add(indexRequest(index).type(type).id(objectId)
							.source(build(data, objectId)).routing(routing)
							.parent(parent));
					updatedDocuments++;
					// new UpdateRequest(indexName, typeName, objectId)
				}
				if (OPLOG_DELETE_OPERATION.equals(operation)) {
					logger.info("Delete request [{}], [{}], [{}]", index, type,
							objectId);
					bulk.add(new DeleteRequest(index, type, objectId).routing(
							routing).parent(parent));
					deletedDocuments++;
				}
				if (OPLOG_COMMAND_OPERATION.equals(operation)) {
					if (dropCollection) {
						if (data.containsKey(OPLOG_DROP_COMMAND_OPERATION)
								&& data.get(OPLOG_DROP_COMMAND_OPERATION)
										.equals(mongoCollection)) {
							logger.info("Drop collection request [{}], [{}]",
									index, type);
							bulk.request().requests().clear();
							client.admin().indices().prepareRefresh(index)
									.execute().actionGet();
							ImmutableMap<String, MappingMetaData> mappings = client
									.admin().cluster().prepareState().execute()
									.actionGet().getState().getMetaData()
									.index(index).mappings();
							logger.trace("mappings contains type {}: {}", type,
									mappings.containsKey(type));
							if (mappings.containsKey(type)) {
								/*
								 * Issue #105 - Mapping changing from custom
								 * mapping to dynamic when drop_collection =
								 * true Should capture the existing mapping
								 * metadata (in case it is has been customized
								 * before to delete.
								 */
								MappingMetaData mapping = mappings.get(type);
								client.admin().indices()
										.prepareDeleteMapping(index)
										.setType(type).execute().actionGet();
								PutMappingResponse pmr = client.admin()
										.indices().preparePutMapping(index)
										.setType(type)
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
			} catch (IOException e) {
				logger.warn("failed to parse {}", e, data);
			}
			return lastTimestamp;
		}

		private XContentBuilder build(final Map<String, Object> data,
				final String objectId) throws IOException {
			if (data.containsKey(IS_MONGODB_ATTACHMENT)) {
				logger.info("Add Attachment: {} to index {} / type {}",
						objectId, indexName, typeName);
				return MongoDBHelper.serialize((GridFSDBFile) data
						.get(MONGODB_ATTACHMENT));
			} else {
				return XContentFactory.jsonBuilder().map(data);
			}
		}

		private String extractObjectId(Map<String, Object> ctx, String objectId) {
			Object id = ctx.get("id");
			if (id == null) {
				return objectId;
			} else {
				return id.toString();
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

		private String extractType(Map<String, Object> ctx) {
			Object type = ctx.get("_type");
			if (type == null) {
				return typeName;
			} else {
				return type.toString();
			}
		}

		private String extractIndex(Map<String, Object> ctx) {
			String index = (String) ctx.get("_index");
			if (index == null) {
				index = indexName;
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
		private final List<ServerAddress> mongoServers;

		public Slurper(List<ServerAddress> mongoServers) {
			this.mongoServers = mongoServers;
		}

		private boolean assignCollections() {
			DB adminDb = mongo.getDB(MONGODB_ADMIN);
			oplogDb = mongo.getDB(MONGODB_LOCAL);

			if (!mongoAdminUser.isEmpty() && !mongoAdminPassword.isEmpty()) {
				logger.info("Authenticate {} with {}", MONGODB_ADMIN,
						mongoAdminUser);

				CommandResult cmd = adminDb.authenticateCommand(mongoAdminUser,
						mongoAdminPassword.toCharArray());
				if (!cmd.ok()) {
					logger.error("Autenticatication failed for {}: {}",
							MONGODB_ADMIN, cmd.getErrorMessage());
					// Can still try with mongoLocal credential if provided.
					// return false;
				}
				oplogDb = adminDb.getMongo().getDB(MONGODB_LOCAL);
			}

			if (!mongoLocalUser.isEmpty() && !mongoLocalPassword.isEmpty()
					&& !oplogDb.isAuthenticated()) {
				logger.info("Authenticate {} with {}", MONGODB_LOCAL,
						mongoLocalUser);
				CommandResult cmd = oplogDb.authenticateCommand(mongoLocalUser,
						mongoLocalPassword.toCharArray());
				if (!cmd.ok()) {
					logger.error("Autenticatication failed for {}: {}",
							MONGODB_LOCAL, cmd.getErrorMessage());
					return false;
				}
			}

			Set<String> collections = oplogDb.getCollectionNames();
			if (!collections.contains(OPLOG_COLLECTION)) {
				logger.error("Cannot find "
						+ OPLOG_COLLECTION
						+ " collection. Please use check this link: http://goo.gl/2x5IW");
				return false;
			}
			oplogCollection = oplogDb.getCollection(OPLOG_COLLECTION);

			slurpedDb = mongo.getDB(mongoDb);
			if (!mongoAdminUser.isEmpty() && !mongoAdminUser.isEmpty()
					&& adminDb.isAuthenticated()) {
				slurpedDb = adminDb.getMongo().getDB(mongoDb);
			}

			// Not necessary as local user has access to all databases.
			// http://docs.mongodb.org/manual/reference/local-database/
			// if (!mongoDbUser.isEmpty() && !mongoDbPassword.isEmpty()
			// && !slurpedDb.isAuthenticated()) {
			// logger.info("Authenticate {} with {}", mongoDb, mongoDbUser);
			// CommandResult cmd = slurpedDb.authenticateCommand(mongoDbUser,
			// mongoDbPassword.toCharArray());
			// if (!cmd.ok()) {
			// logger.error("Autenticatication failed for {}: {}",
			// mongoDb, cmd.getErrorMessage());
			// return false;
			// }
			// }
			slurpedCollection = slurpedDb.getCollection(mongoCollection);

			return true;
		}

		@Override
		public void run() {
			Builder builder = MongoClientOptions.builder()
					.autoConnectRetry(true).connectTimeout(15000)
					.socketKeepAlive(true).socketTimeout(60000);
			if (mongoUseSSL) {
				builder.socketFactory(getSSLSocketFactory());
			}

			// TODO: MongoClientOptions should be configurable
			MongoClientOptions mco = builder.build();
			mongo = new MongoClient(mongoServers, mco);

			if (mongoSecondaryReadPreference) {
				mongo.setReadPreference(ReadPreference.secondaryPreferred());
			}

			while (active) {
				try {
					if (!assignCollections()) {
						break; // failed to assign oplogCollection or
								// slurpedCollection
					}

					DBCursor oplogCursor = oplogCursor(null);
					if (oplogCursor == null) {
						oplogCursor = processFullCollection();
					}

					while (oplogCursor.hasNext()) {
						DBObject item = oplogCursor.next();
						processOplogEntry(item);
					}
					logger.trace("*** Try again in few seconds...");
					Thread.sleep(500);
				} catch (MongoInterruptedException mIEx) {
					logger.error("Mongo driver has been interrupted", mIEx);
					// active = false;
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

		/*
		 * Remove fscynlock and unlock -
		 * https://github.com/richardwilly98/elasticsearch
		 * -river-mongodb/issues/17
		 */
		private DBCursor processFullCollection() throws InterruptedException {
			// CommandResult lockResult = mongo.fsyncAndLock();
			// if (lockResult.ok()) {
			try {
				BSONTimestamp currentTimestamp = (BSONTimestamp) oplogCollection
						.find().sort(new BasicDBObject(OPLOG_TIMESTAMP, -1))
						.limit(1).next().get(OPLOG_TIMESTAMP);
				addQueryToStream(OPLOG_INSERT_OPERATION, currentTimestamp, null);
				return oplogCursor(currentTimestamp);
			} finally {
				// mongo.unlock();
			}
			// } else {
			// throw new MongoException(
			// "Could not lock the database for FullCollection sync");
			// }
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
			if (entry.containsField("fromMigrate")
					&& ((BasicBSONObject) entry).getBoolean("fromMigrate")) {
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
			if (mongoGridFS
					&& namespace.endsWith(GRIDFS_FILES_SUFFIX)
					&& (OPLOG_INSERT_OPERATION.equals(operation) || OPLOG_UPDATE_OPERATION
							.equals(operation))) {
				if (objectId == null) {
					throw new NullPointerException(MONGODB_ID_FIELD);
				}
				GridFS grid = new GridFS(mongo.getDB(mongoDb), mongoCollection);
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
				object = MongoDBHelper
						.applyExcludeFields(object, excludeFields);
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
					object = MongoDBHelper.applyExcludeFields(object,
							excludeFields);
					addToStream(operation, oplogTimestamp, object.toMap());
				}
			}
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

		private DBObject getIndexFilter(final BSONTimestamp timestampOverride) {
			BSONTimestamp time = timestampOverride == null ? getLastTimestamp(mongoOplogNamespace)
					: timestampOverride;
			BasicDBObject filter = new BasicDBObject();
			List<DBObject> values = new ArrayList<DBObject>();
			List<DBObject> values2 = new ArrayList<DBObject>();

			if (mongoGridFS) {
				values.add(new BasicDBObject(OPLOG_NAMESPACE,
						mongoOplogNamespace + GRIDFS_FILES_SUFFIX));
			} else {
				// values.add(new BasicDBObject(OPLOG_NAMESPACE,
				// mongoOplogNamespace));
				values2.add(new BasicDBObject(OPLOG_NAMESPACE,
						mongoOplogNamespace));
				values2.add(new BasicDBObject(OPLOG_NAMESPACE, mongoDb + "."
						+ OPLOG_NAMESPACE_COMMAND));
				values.add(new BasicDBObject(MONGODB_OR_OPERATOR, values2));
			}
			if (!mongoFilter.isEmpty()) {
				values.add(getMongoFilter());
			}
			if (time == null) {
				logger.info("No known previous slurping time for this collection");
			} else {
				values.add(new BasicDBObject(OPLOG_TIMESTAMP,
						new BasicDBObject(QueryOperators.GT, time)));
			}
			filter = new BasicDBObject(MONGODB_AND_OPERATOR, values);
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
			filters2.add((DBObject) JSON.parse(mongoFilter));

			filters.add(new BasicDBObject(MONGODB_AND_OPERATOR, filters2));

			return new BasicDBObject(MONGODB_OR_OPERATOR, filters);
		}

		private DBCursor oplogCursor(final BSONTimestamp timestampOverride) {
			DBObject indexFilter = getIndexFilter(timestampOverride);
			if (indexFilter == null) {
				return null;
			}
			return oplogCollection.find(indexFilter)
					.sort(new BasicDBObject(MONGODB_NATURAL_OPERATOR, 1))
					.addOption(Bytes.QUERYOPTION_TAILABLE)
					.addOption(Bytes.QUERYOPTION_AWAITDATA);
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
			data.put(OPLOG_TIMESTAMP, currentTimestamp);
			data.put(OPLOG_OPERATION, operation);

			// stream.add(data);
			stream.put(data);
			// try {
			// stream.put(data);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
		}

	}

	private class Status implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					if (startInvoked) {
						// logger.trace("*** river status thread waiting: {} ***",
						// riverName.getName());

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
							// active = true;
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
				.startObject(typeName).startObject("properties")
				.startObject("content").field("type", "attachment").endObject()
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
			if (initialTimestamp != null) {
				return initialTimestamp;
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

}
