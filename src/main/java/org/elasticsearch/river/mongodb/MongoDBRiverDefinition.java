package org.elasticsearch.river.mongodb;

import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.bson.types.BSONTimestamp;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class MongoDBRiverDefinition {

	private static final ESLogger logger = Loggers
			.getLogger(MongoDBRiverDefinition.class);

	// defaults
	public final static String DEFAULT_DB_HOST = "localhost";
	public final static int DEFAULT_DB_PORT = 27017;

	// fields
	public final static String DB_FIELD = "db";
	public final static String SERVERS_FIELD = "servers";
	public final static String HOST_FIELD = "host";
	public final static String PORT_FIELD = "port";
	public final static String OPTIONS_FIELD = "options";
	public final static String SECONDARY_READ_PREFERENCE_FIELD = "secondary_read_preference";
	public final static String CONNECTION_TIMEOUT = "connect_timeout";
	public final static String SOCKET_TIMEOUT = "socket_timeout";
	public final static String SSL_CONNECTION_FIELD = "ssl";
	public final static String SSL_VERIFY_CERT_FIELD = "ssl_verify_certificate";
	public final static String DROP_COLLECTION_FIELD = "drop_collection";
	public final static String EXCLUDE_FIELDS_FIELD = "exclude_fields";
	public final static String INCLUDE_FIELDS_FIELD = "include_fields";
	public final static String INCLUDE_COLLECTION_FIELD = "include_collection";
	public final static String INITIAL_TIMESTAMP_FIELD = "initial_timestamp";
	public final static String INITIAL_TIMESTAMP_SCRIPT_TYPE_FIELD = "script_type";
	public final static String INITIAL_TIMESTAMP_SCRIPT_FIELD = "script";
	public final static String ADVANCED_TRANSFORMATION_FIELD = "advanced_transformation";
	public final static String PARENT_TYPES_FIELD = "parent_types";
	public final static String FILTER_FIELD = "filter";
	public final static String CREDENTIALS_FIELD = "credentials";
	public final static String USER_FIELD = "user";
	public final static String PASSWORD_FIELD = "password";
	public final static String SCRIPT_FIELD = "script";
	public final static String SCRIPT_TYPE_FIELD = "script_type";
	public final static String COLLECTION_FIELD = "collection";
	public final static String GRIDFS_FIELD = "gridfs";
	public final static String INDEX_OBJECT = "index";
	public final static String NAME_FIELD = "name";
	public final static String TYPE_FIELD = "type";
	public final static String LOCAL_DB_FIELD = "local";
	public final static String ADMIN_DB_FIELD = "admin";
	public final static String THROTTLE_SIZE_FIELD = "throttle_size";
	public final static String BULK_SIZE_FIELD = "bulk_size";
	public final static String BULK_TIMEOUT_FIELD = "bulk_timeout";

	// mongodb.servers
	private final List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
	// mongodb
	private final String mongoDb;
	private final String mongoCollection;
	private final boolean mongoGridFS;
	private final String mongoFilter;
	// mongodb.credentials
	private final String mongoAdminUser;
	private final String mongoAdminPassword;
	private final String mongoLocalUser;
	private final String mongoLocalPassword;

	// mongodb.options
	private final MongoClientOptions mongoClientOptions;
	private final int connectTimeout;
	private final int socketTimeout;
	private final boolean mongoSecondaryReadPreference;
	private final boolean mongoUseSSL;
	private final boolean mongoSSLVerifyCertificate;
	private final boolean dropCollection;
	private final Set<String> excludeFields;
	private final Set<String> includeFields;
	private final String includeCollection;
	private final BSONTimestamp initialTimestamp;
	private final String script;
	private final String scriptType;
	private final boolean advancedTransformation;
	private final Set<String> parentTypes;
	// index
	private final String indexName;
	private final String typeName;
	private final int bulkSize;
	private final TimeValue bulkTimeout;
	private final int throttleSize;

	public static class Builder {
		// mongodb.servers
		private List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
		// mongodb
		private String mongoDb;
		private String mongoCollection;
		private boolean mongoGridFS;
		private String mongoFilter = "";
		// mongodb.credentials
		private String mongoAdminUser = "";
		private String mongoAdminPassword = "";
		private String mongoLocalUser = "";
		private String mongoLocalPassword = "";
		// mongodb.options
		private MongoClientOptions mongoClientOptions = null;
		private int connectTimeout = 0;
		private int socketTimeout = 0;
		private boolean mongoSecondaryReadPreference = false;
		private boolean mongoUseSSL = false;
		private boolean mongoSSLVerifyCertificate = false;
		private boolean dropCollection = false;
		private Set<String> excludeFields = null;
		private Set<String> includeFields = null;
		private String includeCollection = "";
		private BSONTimestamp initialTimestamp = null;
		private String script = null;
		private String scriptType = null;
		private boolean advancedTransformation = false;
		private Set<String> parentTypes = null;

		// index
		private String indexName;
		private String typeName;
		private int bulkSize;
		private TimeValue bulkTimeout;
		private int throttleSize;

		public Builder mongoServers(List<ServerAddress> mongoServers) {
			this.mongoServers = mongoServers;
			return this;
		}

		public Builder mongoDb(String mongoDb) {
			this.mongoDb = mongoDb;
			return this;
		}

		public Builder mongoCollection(String mongoCollection) {
			this.mongoCollection = mongoCollection;
			return this;
		}

		public Builder mongoGridFS(boolean mongoGridFS) {
			this.mongoGridFS = mongoGridFS;
			return this;
		}

		public Builder mongoFilter(String mongoFilter) {
			this.mongoFilter = mongoFilter;
			return this;
		}

		public Builder mongoAdminUser(String mongoAdminUser) {
			this.mongoAdminUser = mongoAdminUser;
			return this;
		}

		public Builder mongoAdminPassword(String mongoAdminPassword) {
			this.mongoAdminPassword = mongoAdminPassword;
			return this;
		}

		public Builder mongoLocalUser(String mongoLocalUser) {
			this.mongoLocalUser = mongoLocalUser;
			return this;
		}

		public Builder mongoLocalPassword(String mongoLocalPassword) {
			this.mongoLocalPassword = mongoLocalPassword;
			return this;
		}

		public Builder mongoClientOptions(MongoClientOptions mongoClientOptions) {
			this.mongoClientOptions = mongoClientOptions;
			return this;
		}

		public Builder connectTimeout(int connectTimeout) {
			this.connectTimeout = connectTimeout;
			return this;
		}

		public Builder socketTimeout(int socketTimeout) {
			this.socketTimeout = socketTimeout;
			return this;
		}

		public Builder mongoSecondaryReadPreference(
				boolean mongoSecondaryReadPreference) {
			this.mongoSecondaryReadPreference = mongoSecondaryReadPreference;
			return this;
		}

		public Builder mongoUseSSL(boolean mongoUseSSL) {
			this.mongoUseSSL = mongoUseSSL;
			return this;
		}

		public Builder mongoSSLVerifyCertificate(
				boolean mongoSSLVerifyCertificate) {
			this.mongoSSLVerifyCertificate = mongoSSLVerifyCertificate;
			return this;
		}

		public Builder dropCollection(boolean dropCollection) {
			this.dropCollection = dropCollection;
			return this;
		}

		public Builder excludeFields(Set<String> excludeFields) {
			this.excludeFields = excludeFields;
			return this;
		}

		public Builder includeFields(Set<String> includeFields) {
			this.includeFields = includeFields;
			return this;
		}

		public Builder includeCollection(String includeCollection) {
			this.includeCollection = includeCollection;
			return this;
		}

		public Builder initialTimestamp(BSONTimestamp initialTimestamp) {
			this.initialTimestamp = initialTimestamp;
			return this;
		}

		public Builder advancedTransformation(boolean advancedTransformation) {
			this.advancedTransformation = advancedTransformation;
			return this;
		}

		public Builder parentTypes(Set<String> parentTypes) {
			this.parentTypes = parentTypes;
			return this;
		}

		public Builder script(String script) {
			this.script = script;
			return this;
		}

		public Builder scriptType(String scriptType) {
			this.scriptType = scriptType;
			return this;
		}

		public Builder indexName(String indexName) {
			this.indexName = indexName;
			return this;
		}

		public Builder typeName(String typeName) {
			this.typeName = typeName;
			return this;
		}

		public Builder bulkSize(int bulkSize) {
			this.bulkSize = bulkSize;
			return this;
		}

		public Builder bulkTimeout(TimeValue bulkTimeout) {
			this.bulkTimeout = bulkTimeout;
			return this;
		}

		public Builder throttleSize(int throttleSize) {
			this.throttleSize = throttleSize;
			return this;
		}

		public MongoDBRiverDefinition build() {
			return new MongoDBRiverDefinition(this);
		}
	}

	@SuppressWarnings("unchecked")
	public synchronized static MongoDBRiverDefinition parseSettings(
			final RiverName riverName, final RiverSettings settings,
			final ScriptService scriptService) {

		Preconditions.checkNotNull(riverName, "No riverName specified");
		Preconditions.checkNotNull(settings, "No settings specified");

		Builder builder = new Builder();
		List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
		String mongoHost;
		int mongoPort;

		if (settings.settings().containsKey(MongoDBRiver.TYPE)) {
			Map<String, Object> mongoSettings = (Map<String, Object>) settings
					.settings().get(MongoDBRiver.TYPE);
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
						} catch (UnknownHostException uhEx) {
							logger.warn("Cannot add mongo server {}:{}", uhEx,
									mongoHost, mongoPort);
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
				} catch (UnknownHostException uhEx) {
					logger.warn("Cannot add mongo server {}:{}", uhEx,
							mongoHost, mongoPort);
				}
			}
			builder.mongoServers(mongoServers);

			MongoClientOptions.Builder mongoClientOptionsBuilder = MongoClientOptions
					.builder().autoConnectRetry(true)
					.socketKeepAlive(true);
			
			// MongoDB options
			if (mongoSettings.containsKey(OPTIONS_FIELD)) {
				Map<String, Object> mongoOptionsSettings = (Map<String, Object>) mongoSettings
						.get(OPTIONS_FIELD);
				logger.trace("mongoOptionsSettings: " + mongoOptionsSettings);
				builder.mongoSecondaryReadPreference(XContentMapValues
						.nodeBooleanValue(mongoOptionsSettings
								.get(SECONDARY_READ_PREFERENCE_FIELD), false));
				builder.connectTimeout(XContentMapValues.nodeIntegerValue(
						mongoOptionsSettings.get(CONNECTION_TIMEOUT), 0));
				builder.socketTimeout(XContentMapValues.nodeIntegerValue(
						mongoOptionsSettings.get(SOCKET_TIMEOUT), 0));
				builder.dropCollection(XContentMapValues.nodeBooleanValue(
						mongoOptionsSettings.get(DROP_COLLECTION_FIELD), false));
				builder.mongoUseSSL(XContentMapValues.nodeBooleanValue(
						mongoOptionsSettings.get(SSL_CONNECTION_FIELD), false));
				builder.mongoSSLVerifyCertificate(XContentMapValues.nodeBooleanValue(
						mongoOptionsSettings.get(SSL_VERIFY_CERT_FIELD), true));
				builder.advancedTransformation(XContentMapValues
						.nodeBooleanValue(mongoOptionsSettings
								.get(ADVANCED_TRANSFORMATION_FIELD), false));

				mongoClientOptionsBuilder
						.connectTimeout(builder.connectTimeout)
						.socketTimeout(builder.socketTimeout);
				
				if (builder.mongoSecondaryReadPreference) {
					mongoClientOptionsBuilder.readPreference(ReadPreference.secondaryPreferred());
				}
				
				if (builder.mongoUseSSL) {
					mongoClientOptionsBuilder
							.socketFactory(getSSLSocketFactory());
				}

				if (mongoOptionsSettings.containsKey(PARENT_TYPES_FIELD)) {
					Set<String> parentTypes = new HashSet<String>();
					Object parentTypesSettings = mongoOptionsSettings
							.get(PARENT_TYPES_FIELD);
					logger.info("parentTypesSettings: " + parentTypesSettings);
					boolean array = XContentMapValues
							.isArray(parentTypesSettings);

					if (array) {
						ArrayList<String> fields = (ArrayList<String>) parentTypesSettings;
						for (String field : fields) {
							logger.info("Field: " + field);
							parentTypes.add(field);
						}
					}

					builder.parentTypes(parentTypes);
				}

				builder.includeCollection(XContentMapValues.nodeStringValue(
						mongoOptionsSettings.get(INCLUDE_COLLECTION_FIELD), ""));

				if (mongoOptionsSettings.containsKey(INCLUDE_FIELDS_FIELD)) {
					Set<String> includeFields = new HashSet<String>();
					Object includeFieldsSettings = mongoOptionsSettings
							.get(INCLUDE_FIELDS_FIELD);
					logger.info("includeFieldsSettings: "
							+ includeFieldsSettings);
					boolean array = XContentMapValues
							.isArray(includeFieldsSettings);

					if (array) {
						ArrayList<String> fields = (ArrayList<String>) includeFieldsSettings;
						for (String field : fields) {
							logger.info("Field: " + field);
							includeFields.add(field);
						}
					}

					if (!includeFields.contains(MongoDBRiver.MONGODB_ID_FIELD)) {
						includeFields.add(MongoDBRiver.MONGODB_ID_FIELD);
					}
					builder.includeFields(includeFields);
				} else if (mongoOptionsSettings
						.containsKey(EXCLUDE_FIELDS_FIELD)) {
					Set<String> excludeFields = new HashSet<String>();
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

					builder.excludeFields(excludeFields);
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
						builder.initialTimestamp(timeStamp);
					}
				}
			}
			builder.mongoClientOptions(mongoClientOptionsBuilder.build());

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
				builder.mongoAdminUser(mau);
				builder.mongoAdminPassword(map);
				builder.mongoLocalUser(mlu);
				builder.mongoLocalPassword(mlp);
				// mongoDbUser = mdu;
				// mongoDbPassword = mdp;
			}

			builder.mongoDb(XContentMapValues.nodeStringValue(
					mongoSettings.get(DB_FIELD), riverName.name()));
			builder.mongoCollection(XContentMapValues.nodeStringValue(
					mongoSettings.get(COLLECTION_FIELD), riverName.name()));
			builder.mongoGridFS(XContentMapValues.nodeBooleanValue(
					mongoSettings.get(GRIDFS_FIELD), false));
			if (mongoSettings.containsKey(FILTER_FIELD)) {
				builder.mongoFilter(XContentMapValues.nodeStringValue(
						mongoSettings.get(FILTER_FIELD), ""));
			} else {
				builder.mongoFilter("");
			}

			if (mongoSettings.containsKey(SCRIPT_FIELD)) {
				String scriptType = "js";
				builder.script(mongoSettings.get(SCRIPT_FIELD).toString());
				if (mongoSettings.containsKey("scriptType")) {
					scriptType = mongoSettings.get("scriptType").toString();
				} else if (mongoSettings.containsKey(SCRIPT_TYPE_FIELD)) {
					scriptType = mongoSettings.get(SCRIPT_TYPE_FIELD)
							.toString();
				}
				builder.scriptType(scriptType);
			}
		} else {
			mongoHost = DEFAULT_DB_HOST;
			mongoPort = DEFAULT_DB_PORT;
			try {
				mongoServers.add(new ServerAddress(mongoHost, mongoPort));
				builder.mongoServers(mongoServers);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			builder.mongoDb(riverName.name());
			builder.mongoCollection(riverName.name());
		}

		if (settings.settings().containsKey(INDEX_OBJECT)) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings
					.settings().get(INDEX_OBJECT);
			builder.indexName(XContentMapValues.nodeStringValue(
					indexSettings.get(NAME_FIELD), builder.mongoDb));
			builder.typeName(XContentMapValues.nodeStringValue(
					indexSettings.get(TYPE_FIELD), builder.mongoDb));
			int bulkSize = XContentMapValues.nodeIntegerValue(
					indexSettings.get(BULK_SIZE_FIELD), 100);
			builder.bulkSize(bulkSize);
			if (indexSettings.containsKey(BULK_TIMEOUT_FIELD)) {
				builder.bulkTimeout(TimeValue.parseTimeValue(
						XContentMapValues.nodeStringValue(
								indexSettings.get(BULK_TIMEOUT_FIELD), "10ms"),
						TimeValue.timeValueMillis(10)));
			} else {
				builder.bulkTimeout(TimeValue.timeValueMillis(10));
			}
			builder.throttleSize(XContentMapValues.nodeIntegerValue(
					indexSettings.get(THROTTLE_SIZE_FIELD), bulkSize * 5));
		} else {
			builder.indexName(builder.mongoDb);
			builder.typeName(builder.mongoDb);
			builder.bulkSize(100);
			builder.bulkTimeout(TimeValue.timeValueMillis(10));
			builder.throttleSize(builder.bulkSize * 5);
		}
		return builder.build();
	}

	private static SocketFactory getSSLSocketFactory() {
		SocketFactory sslSocketFactory;
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
		return SSLSocketFactory.getDefault();
	}

	private MongoDBRiverDefinition(final Builder builder) {
		this.mongoServers.addAll(builder.mongoServers);
		// mongodb
		this.mongoDb = builder.mongoDb;
		this.mongoCollection = builder.mongoCollection;
		this.mongoGridFS = builder.mongoGridFS;
		this.mongoFilter = builder.mongoFilter;
		// mongodb.credentials
		this.mongoAdminUser = builder.mongoAdminUser;
		this.mongoAdminPassword = builder.mongoAdminPassword;
		this.mongoLocalUser = builder.mongoLocalUser;
		this.mongoLocalPassword = builder.mongoLocalPassword;

		// mongodb.options
		this.mongoClientOptions = builder.mongoClientOptions;
		this.connectTimeout = builder.connectTimeout;
		this.socketTimeout = builder.socketTimeout;
		this.mongoSecondaryReadPreference = builder.mongoSecondaryReadPreference;
		this.mongoUseSSL = builder.mongoUseSSL;
		this.mongoSSLVerifyCertificate = builder.mongoSSLVerifyCertificate;
		this.dropCollection = builder.dropCollection;
		this.excludeFields = builder.excludeFields;
		this.includeFields = builder.includeFields;
		this.includeCollection = builder.includeCollection;
		this.initialTimestamp = builder.initialTimestamp;
		this.script = builder.script;
		this.scriptType = builder.scriptType;
		this.advancedTransformation = builder.advancedTransformation;
		this.parentTypes = builder.parentTypes;

		// index
		this.indexName = builder.indexName;
		this.typeName = builder.typeName;
		this.bulkSize = builder.bulkSize;
		this.bulkTimeout = builder.bulkTimeout;
		this.throttleSize = builder.throttleSize;

	}

	public List<ServerAddress> getMongoServers() {
		return mongoServers;
	}

	public String getMongoDb() {
		return mongoDb;
	}

	public String getMongoCollection() {
		return mongoCollection;
	}

	public boolean isMongoGridFS() {
		return mongoGridFS;
	}

	public String getMongoFilter() {
		return mongoFilter;
	}

	public String getMongoAdminUser() {
		return mongoAdminUser;
	}

	public String getMongoAdminPassword() {
		return mongoAdminPassword;
	}

	public String getMongoLocalUser() {
		return mongoLocalUser;
	}

	public String getMongoLocalPassword() {
		return mongoLocalPassword;
	}

	public MongoClientOptions getMongoClientOptions() {
		return mongoClientOptions;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public boolean isMongoSecondaryReadPreference() {
		return mongoSecondaryReadPreference;
	}

	public boolean isMongoUseSSL() {
		return mongoUseSSL;
	}

	public boolean isMongoSSLVerifyCertificate() {
		return mongoSSLVerifyCertificate;
	}

	public boolean isDropCollection() {
		return dropCollection;
	}

	public Set<String> getExcludeFields() {
		return excludeFields;
	}

	public Set<String> getIncludeFields() {
		return includeFields;
	}

	public String getIncludeCollection() {
		return includeCollection;
	}

	public BSONTimestamp getInitialTimestamp() {
		return initialTimestamp;
	}

	public String getScript() {
		return script;
	}

	public String getScriptType() {
		return scriptType;
	}

	public boolean isAdvancedTransformation() {
		return advancedTransformation;
	}

	public Set<String> getParentTypes() {
		return parentTypes;
	}

	public String getIndexName() {
		return indexName;
	}

	public String getTypeName() {
		return typeName;
	}

	public int getBulkSize() {
		return bulkSize;
	}

	public TimeValue getBulkTimeout() {
		return bulkTimeout;
	}

	public int getThrottleSize() {
		return throttleSize;
	}

}
