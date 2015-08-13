package org.elasticsearch.river.mongodb;

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

import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

public class MongoDBRiverDefinition {

    private static final ESLogger logger = Loggers.getLogger(MongoDBRiverDefinition.class);

    // defaults
    public final static String DEFAULT_DB_HOST = "localhost";
    public final static int DEFAULT_DB_PORT = 27017;
    public final static int DEFAULT_CONCURRENT_REQUESTS = Runtime.getRuntime().availableProcessors();
    public final static int DEFAULT_BULK_ACTIONS = 1000;
    public final static TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueMillis(10);
    public final static ByteSizeValue DEFAULT_BULK_SIZE = new ByteSizeValue(5, ByteSizeUnit.MB);
    public final static int DEFAULT_CONNECT_TIMEOUT = 30000;
    public final static int DEFAULT_SOCKET_TIMEOUT = 60000;
    public final static int DEFAULT_CONNECTIONS_PER_HOST = 100;
    public final static int DEFAULT_THREADS_ALLOWED_TO_BLOCK_FOR_CONNECTION_MULTIPLIER = 5;

    // fields
    public final static String DB_FIELD = "db";
    public final static String SERVERS_FIELD = "servers";
    public final static String HOST_FIELD = "host";
    public final static String PORT_FIELD = "port";
    public final static String OPTIONS_FIELD = "options";
    public final static String SECONDARY_READ_PREFERENCE_FIELD = "secondary_read_preference";
    public final static String CONNECT_TIMEOUT = "connect_timeout";
    public final static String SOCKET_TIMEOUT = "socket_timeout";
    public final static String SSL_CONNECTION_FIELD = "ssl";
    public final static String SSL_VERIFY_CERT_FIELD = "ssl_verify_certificate";
    public final static String IS_MONGOS_FIELD = "is_mongos";
    public final static String DROP_COLLECTION_FIELD = "drop_collection";
    public final static String EXCLUDE_FIELDS_FIELD = "exclude_fields";
    public final static String INCLUDE_FIELDS_FIELD = "include_fields";
    public final static String INCLUDE_COLLECTION_FIELD = "include_collection";
    public final static String INITIAL_TIMESTAMP_FIELD = "initial_timestamp";
    public final static String INITIAL_TIMESTAMP_SCRIPT_TYPE_FIELD = "script_type";
    public final static String INITIAL_TIMESTAMP_SCRIPT_FIELD = "script";
    public final static String ADVANCED_TRANSFORMATION_FIELD = "advanced_transformation";
    public final static String SKIP_INITIAL_IMPORT_FIELD = "skip_initial_import";
    public final static String CONNECTIONS_PER_HOST = "connections_per_host";
    public final static String THREADS_ALLOWED_TO_BLOCK_FOR_CONNECTION_MULTIPLIER = "threads_allowed_to_block_for_connection_multiplier";
    public final static String PARENT_TYPES_FIELD = "parent_types";
    public final static String STORE_STATISTICS_FIELD = "store_statistics";
    public final static String IMPORT_ALL_COLLECTIONS_FIELD = "import_all_collections";
    public final static String DISABLE_INDEX_REFRESH_FIELD = "disable_index_refresh";
    public final static String FILTER_FIELD = "filter";
    public final static String CREDENTIALS_FIELD = "credentials";
    public final static String USER_FIELD = "user";
    public final static String PASSWORD_FIELD = "password";
    public final static String AUTH_FIELD = "auth";
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
    public final static String CONCURRENT_BULK_REQUESTS_FIELD = "concurrent_bulk_requests";

    public final static String BULK_FIELD = "bulk";
    public final static String ACTIONS_FIELD = "actions";
    public final static String SIZE_FIELD = "size";
    public final static String CONCURRENT_REQUESTS_FIELD = "concurrent_requests";
    public final static String FLUSH_INTERVAL_FIELD = "flush_interval";

    // river
    private final String riverName;
    private final String riverIndexName;

    // mongodb.servers
    private final List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
    // mongodb
    private final String mongoDb;
    private final String mongoCollection;
    private final boolean mongoGridFS;
    private final BasicDBObject mongoOplogFilter;
    private final BasicDBObject mongoCollectionFilter;
    // mongodb.credentials
    private final String mongoAdminUser;
    private final String mongoAdminPassword;
    private final String mongoAdminAuthDatabase;
    private final String mongoLocalUser;
    private final String mongoLocalPassword;
    private final String mongoLocalAuthDatabase;

    // mongodb.options
    private final MongoClientOptions mongoClientOptions;
    private final int connectTimeout;
    private final int socketTimeout;
    private final boolean mongoSecondaryReadPreference;
    private final boolean mongoUseSSL;
    private final boolean mongoSSLVerifyCertificate;
    private final boolean dropCollection;
    private final Boolean isMongos;
    private final Set<String> excludeFields;
    private final Set<String> includeFields;
    private final String includeCollection;
    private final Timestamp<?> initialTimestamp;
    private final String script;
    private final String scriptType;
    private final boolean advancedTransformation;
    private final boolean skipInitialImport;
    private final Set<String> parentTypes;
    private final boolean storeStatistics;
    private final String statisticsIndexName;
    private final String statisticsTypeName;
    private final boolean importAllCollections;
    private final boolean disableIndexRefresh;
    // index
    private final String indexName;
    private final String typeName;
    private final int throttleSize;

    // bulk
    private final Bulk bulk;

    public static class Builder {
        // river
        private String riverName;
        private String riverIndexName;

        // mongodb.servers
        private List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
        // mongodb
        private String mongoDb;
        private String mongoCollection;
        private boolean mongoGridFS;
        private BasicDBObject mongoOplogFilter;// = new BasicDBObject();
        private BasicDBObject mongoCollectionFilter = new BasicDBObject();
        // mongodb.credentials
        private String mongoAdminUser = "";
        private String mongoAdminPassword = "";
        private String mongoAdminAuthDatabase = "";
        private String mongoLocalUser = "";
        private String mongoLocalPassword = "";
        private String mongoLocalAuthDatabase = "";
        // mongodb.options
        private MongoClientOptions mongoClientOptions = null;
        private int connectTimeout = 0;
        private int socketTimeout = 0;
        private boolean mongoSecondaryReadPreference = false;
        private boolean mongoUseSSL = false;
        private boolean mongoSSLVerifyCertificate = false;
        private boolean dropCollection = false;
        private Boolean isMongos = null;
        private Set<String> excludeFields = null;
        private Set<String> includeFields = null;
        private String includeCollection = "";
        private Timestamp<?> initialTimestamp = null;
        private String script = null;
        private String scriptType = null;
        private boolean advancedTransformation = false;
        private boolean skipInitialImport;
        private Set<String> parentTypes = null;
        private boolean storeStatistics;
        private String statisticsIndexName;
        private String statisticsTypeName;
        private boolean importAllCollections;
        private boolean disableIndexRefresh;

        // index
        private String indexName;
        private String typeName;
        private int throttleSize;

        private Bulk bulk;
        private int connectionsPerHost;
        private int threadsAllowedToBlockForConnectionMultiplier;

        public Builder mongoServers(List<ServerAddress> mongoServers) {
            this.mongoServers = mongoServers;
            return this;
        }

        public Builder riverName(String riverName) {
            this.riverName = riverName;
            return this;
        }

        public Builder riverIndexName(String riverIndexName) {
            this.riverIndexName = riverIndexName;
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

        public Builder mongoOplogFilter(BasicDBObject mongoOplogFilter) {
            this.mongoOplogFilter = mongoOplogFilter;
            return this;
        }

        public Builder mongoCollectionFilter(BasicDBObject mongoCollectionFilter) {
            this.mongoCollectionFilter = mongoCollectionFilter;
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
        
        public Builder mongoAdminAuthDatabase(String mongoAdminAuthDatabase) {
            this.mongoAdminAuthDatabase = mongoAdminAuthDatabase;
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
        
        public Builder mongoLocalAuthDatabase(String mongoLocalAuthDatabase) {
            this.mongoLocalAuthDatabase = mongoLocalAuthDatabase;
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

        public Builder mongoSecondaryReadPreference(boolean mongoSecondaryReadPreference) {
            this.mongoSecondaryReadPreference = mongoSecondaryReadPreference;
            return this;
        }

        public Builder mongoUseSSL(boolean mongoUseSSL) {
            this.mongoUseSSL = mongoUseSSL;
            return this;
        }

        public Builder mongoSSLVerifyCertificate(boolean mongoSSLVerifyCertificate) {
            this.mongoSSLVerifyCertificate = mongoSSLVerifyCertificate;
            return this;
        }

        public Builder dropCollection(boolean dropCollection) {
            this.dropCollection = dropCollection;
            return this;
        }

        public Builder isMongos(Boolean isMongos) {
            this.isMongos = isMongos;
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

        public Builder disableIndexRefresh(boolean disableIndexRefresh) {
            this.disableIndexRefresh = disableIndexRefresh;
            return this;
        }

        public Builder initialTimestamp(Binary initialTimestamp) {
            this.initialTimestamp = new Timestamp.GTID(initialTimestamp.getData(), null);
            return this;
        }

        public Builder initialTimestamp(BSONTimestamp initialTimestamp) {
            this.initialTimestamp = new Timestamp.BSON(initialTimestamp);
            return this;
        }

        public Builder advancedTransformation(boolean advancedTransformation) {
            this.advancedTransformation = advancedTransformation;
            return this;
        }

        public Builder skipInitialImport(boolean skipInitialImport) {
            this.skipInitialImport = skipInitialImport;
            return this;
        }

        public Builder parentTypes(Set<String> parentTypes) {
            this.parentTypes = parentTypes;
            return this;
        }

        public Builder storeStatistics(boolean storeStatistics) {
            this.storeStatistics = storeStatistics;
            return this;
        }

        public Builder statisticsIndexName(String statisticsIndexName) {
            this.statisticsIndexName = statisticsIndexName;
            return this;
        }

        public Builder statisticsTypeName(String statisticsTypeName) {
            this.statisticsTypeName = statisticsTypeName;
            return this;
        }

        public Builder importAllCollections(boolean importAllCollections) {
            this.importAllCollections = importAllCollections;
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

        public Builder throttleSize(int throttleSize) {
            this.throttleSize = throttleSize;
            return this;
        }

        public Builder bulk(Bulk bulk) {
            this.bulk = bulk;
            return this;
        }

        public Builder connectionsPerHost(int connectionsPerHost) {
            this.connectionsPerHost = connectionsPerHost;
            return this;
        }

        public Builder threadsAllowedToBlockForConnectionMultiplier(int threadsAllowedToBlockForConnectionMultiplier) {
            this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
            return this;
        }

        public MongoDBRiverDefinition build() {
            return new MongoDBRiverDefinition(this);
        }
    }

    static class Bulk {

        private final int concurrentRequests;
        private final int bulkActions;
        private final ByteSizeValue bulkSize;
        private final TimeValue flushInterval;

        static class Builder {

            private int concurrentRequests = DEFAULT_CONCURRENT_REQUESTS;
            private int bulkActions = DEFAULT_BULK_ACTIONS;
            private ByteSizeValue bulkSize = DEFAULT_BULK_SIZE;
            private TimeValue flushInterval = DEFAULT_FLUSH_INTERVAL;

            public Builder concurrentRequests(int concurrentRequests) {
                this.concurrentRequests = concurrentRequests;
                return this;
            }

            public Builder bulkActions(int bulkActions) {
                this.bulkActions = bulkActions;
                return this;
            }

            public Builder bulkSize(ByteSizeValue bulkSize) {
                this.bulkSize = bulkSize;
                return this;
            }

            public Builder flushInterval(TimeValue flushInterval) {
                this.flushInterval = flushInterval;
                return this;
            }

            /**
             * Builds a new bulk processor.
             */
            public Bulk build() {
                return new Bulk(this);
            }
        }

        public Bulk(final Builder builder) {
            this.bulkActions = builder.bulkActions;
            this.bulkSize = builder.bulkSize;
            this.concurrentRequests = builder.concurrentRequests;
            this.flushInterval = builder.flushInterval;
        }

        public int getConcurrentRequests() {
            return concurrentRequests;
        }

        public int getBulkActions() {
            return bulkActions;
        }

        public ByteSizeValue getBulkSize() {
            return bulkSize;
        }

        public TimeValue getFlushInterval() {
            return flushInterval;
        }

    }

    @SuppressWarnings("unchecked")
    public synchronized static MongoDBRiverDefinition parseSettings(String riverName, String riverIndexName, RiverSettings settings,
            ScriptService scriptService) {

        logger.trace("Parse river settings for {}", riverName);
        Preconditions.checkNotNull(riverName, "No riverName specified");
        Preconditions.checkNotNull(riverIndexName, "No riverIndexName specified");
        Preconditions.checkNotNull(settings, "No settings specified");

        Builder builder = new Builder();
        builder.riverName(riverName);
        builder.riverIndexName(riverIndexName);

        List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
        String mongoHost;
        int mongoPort;

        if (settings.settings().containsKey(MongoDBRiver.TYPE)) {
            Map<String, Object> mongoSettings = (Map<String, Object>) settings.settings().get(MongoDBRiver.TYPE);
            if (mongoSettings.containsKey(SERVERS_FIELD)) {
                Object mongoServersSettings = mongoSettings.get(SERVERS_FIELD);
                logger.trace("mongoServersSettings: " + mongoServersSettings);
                boolean array = XContentMapValues.isArray(mongoServersSettings);

                if (array) {
                    ArrayList<Map<String, Object>> feeds = (ArrayList<Map<String, Object>>) mongoServersSettings;
                    for (Map<String, Object> feed : feeds) {
                        mongoHost = XContentMapValues.nodeStringValue(feed.get(HOST_FIELD), null);
                        mongoPort = XContentMapValues.nodeIntegerValue(feed.get(PORT_FIELD), DEFAULT_DB_PORT);
                        logger.trace("Server: " + mongoHost + " - " + mongoPort);
                        mongoServers.add(new ServerAddress(mongoHost, mongoPort));
                    }
                }
            } else {
                mongoHost = XContentMapValues.nodeStringValue(mongoSettings.get(HOST_FIELD), DEFAULT_DB_HOST);
                mongoPort = XContentMapValues.nodeIntegerValue(mongoSettings.get(PORT_FIELD), DEFAULT_DB_PORT);
                mongoServers.add(new ServerAddress(mongoHost, mongoPort));
            }
            builder.mongoServers(mongoServers);

            MongoClientOptions.Builder mongoClientOptionsBuilder = MongoClientOptions.builder()
                    .socketKeepAlive(true);

            // MongoDB options
            if (mongoSettings.containsKey(OPTIONS_FIELD)) {
                Map<String, Object> mongoOptionsSettings = (Map<String, Object>) mongoSettings.get(OPTIONS_FIELD);
                logger.trace("mongoOptionsSettings: " + mongoOptionsSettings);
                builder.mongoSecondaryReadPreference(XContentMapValues.nodeBooleanValue(
                        mongoOptionsSettings.get(SECONDARY_READ_PREFERENCE_FIELD), false));
                builder.connectTimeout(XContentMapValues.nodeIntegerValue(mongoOptionsSettings.get(CONNECT_TIMEOUT),
                        DEFAULT_CONNECT_TIMEOUT));
                builder.socketTimeout(XContentMapValues.nodeIntegerValue(mongoOptionsSettings.get(SOCKET_TIMEOUT), DEFAULT_SOCKET_TIMEOUT));
                builder.dropCollection(XContentMapValues.nodeBooleanValue(mongoOptionsSettings.get(DROP_COLLECTION_FIELD), false));
                String isMongos = XContentMapValues.nodeStringValue(mongoOptionsSettings.get(IS_MONGOS_FIELD), null);
                if (isMongos != null) {
                    builder.isMongos(Boolean.valueOf(isMongos));
                }
                builder.mongoUseSSL(XContentMapValues.nodeBooleanValue(mongoOptionsSettings.get(SSL_CONNECTION_FIELD), false));
                builder.mongoSSLVerifyCertificate(XContentMapValues.nodeBooleanValue(mongoOptionsSettings.get(SSL_VERIFY_CERT_FIELD), true));
                builder.advancedTransformation(XContentMapValues.nodeBooleanValue(mongoOptionsSettings.get(ADVANCED_TRANSFORMATION_FIELD),
                        false));
                builder.skipInitialImport(XContentMapValues.nodeBooleanValue(mongoOptionsSettings.get(SKIP_INITIAL_IMPORT_FIELD), false));
                builder.connectionsPerHost(XContentMapValues.nodeIntegerValue(mongoOptionsSettings.get(CONNECTIONS_PER_HOST), DEFAULT_CONNECTIONS_PER_HOST));
                builder.threadsAllowedToBlockForConnectionMultiplier(XContentMapValues.nodeIntegerValue(mongoOptionsSettings.get(THREADS_ALLOWED_TO_BLOCK_FOR_CONNECTION_MULTIPLIER),
                        DEFAULT_THREADS_ALLOWED_TO_BLOCK_FOR_CONNECTION_MULTIPLIER));

                mongoClientOptionsBuilder
                    .connectTimeout(builder.connectTimeout)
                    .socketTimeout(builder.socketTimeout)
                    .connectionsPerHost(builder.connectionsPerHost)
                    .threadsAllowedToBlockForConnectionMultiplier(builder.threadsAllowedToBlockForConnectionMultiplier);

                if (builder.mongoSecondaryReadPreference) {
                    mongoClientOptionsBuilder.readPreference(ReadPreference.secondaryPreferred());
                }

                if (builder.mongoUseSSL) {
                    mongoClientOptionsBuilder.socketFactory(getSSLSocketFactory());
                }

                if (mongoOptionsSettings.containsKey(PARENT_TYPES_FIELD)) {
                    Set<String> parentTypes = new HashSet<String>();
                    Object parentTypesSettings = mongoOptionsSettings.get(PARENT_TYPES_FIELD);
                    logger.trace("parentTypesSettings: " + parentTypesSettings);
                    boolean array = XContentMapValues.isArray(parentTypesSettings);

                    if (array) {
                        ArrayList<String> fields = (ArrayList<String>) parentTypesSettings;
                        for (String field : fields) {
                            logger.trace("Field: " + field);
                            parentTypes.add(field);
                        }
                    }

                    builder.parentTypes(parentTypes);
                }

                if (mongoOptionsSettings.containsKey(STORE_STATISTICS_FIELD)) {
                    Object storeStatistics = mongoOptionsSettings.get(STORE_STATISTICS_FIELD);
                    boolean object = XContentMapValues.isObject(storeStatistics);
                    if (object) {
                        Map<String, Object> storeStatisticsSettings = (Map<String, Object>) storeStatistics;
                        builder.storeStatistics(true);
                        builder.statisticsIndexName(XContentMapValues.nodeStringValue(storeStatisticsSettings.get(INDEX_OBJECT), riverName
                                + "-stats"));
                        builder.statisticsTypeName(XContentMapValues.nodeStringValue(storeStatisticsSettings.get(TYPE_FIELD), "stats"));
                    } else {
                        builder.storeStatistics(XContentMapValues.nodeBooleanValue(storeStatistics, false));
                        if (builder.storeStatistics) {
                            builder.statisticsIndexName(riverName + "-stats");
                            builder.statisticsTypeName("stats");
                        }
                    }
                }
                // builder.storeStatistics(XContentMapValues.nodeBooleanValue(mongoOptionsSettings.get(STORE_STATISTICS_FIELD),
                // false));
                builder.importAllCollections(XContentMapValues.nodeBooleanValue(mongoOptionsSettings.get(IMPORT_ALL_COLLECTIONS_FIELD),
                        false));
                builder.disableIndexRefresh(XContentMapValues.nodeBooleanValue(mongoOptionsSettings.get(DISABLE_INDEX_REFRESH_FIELD), false));
                builder.includeCollection(XContentMapValues.nodeStringValue(mongoOptionsSettings.get(INCLUDE_COLLECTION_FIELD), ""));

                if (mongoOptionsSettings.containsKey(INCLUDE_FIELDS_FIELD)) {
                    Set<String> includeFields = new HashSet<String>();
                    Object includeFieldsSettings = mongoOptionsSettings.get(INCLUDE_FIELDS_FIELD);
                    logger.trace("includeFieldsSettings: " + includeFieldsSettings);
                    boolean array = XContentMapValues.isArray(includeFieldsSettings);

                    if (array) {
                        ArrayList<String> fields = (ArrayList<String>) includeFieldsSettings;
                        for (String field : fields) {
                            logger.trace("Field: " + field);
                            includeFields.add(field);
                        }
                    }

                    if (!includeFields.contains(MongoDBRiver.MONGODB_ID_FIELD)) {
                        includeFields.add(MongoDBRiver.MONGODB_ID_FIELD);
                    }
                    builder.includeFields(includeFields);
                } else if (mongoOptionsSettings.containsKey(EXCLUDE_FIELDS_FIELD)) {
                    Set<String> excludeFields = new HashSet<String>();
                    Object excludeFieldsSettings = mongoOptionsSettings.get(EXCLUDE_FIELDS_FIELD);
                    logger.trace("excludeFieldsSettings: " + excludeFieldsSettings);
                    boolean array = XContentMapValues.isArray(excludeFieldsSettings);

                    if (array) {
                        ArrayList<String> fields = (ArrayList<String>) excludeFieldsSettings;
                        for (String field : fields) {
                            logger.trace("Field: " + field);
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
                        if (initalTimestampSettings.containsKey(INITIAL_TIMESTAMP_SCRIPT_TYPE_FIELD)) {
                            scriptType = initalTimestampSettings.get(INITIAL_TIMESTAMP_SCRIPT_TYPE_FIELD).toString();
                        }
                        if (initalTimestampSettings.containsKey(INITIAL_TIMESTAMP_SCRIPT_FIELD)) {

                            ExecutableScript scriptExecutable = scriptService.executable(scriptType,
                                    initalTimestampSettings.get(INITIAL_TIMESTAMP_SCRIPT_FIELD).toString(), ScriptService.ScriptType.INLINE, Maps.<String, Object>newHashMap());
                            Object ctx = scriptExecutable.run();
                            logger.trace("initialTimestamp script returned: {}", ctx);
                            if (ctx != null) {
                                long timestamp = Long.parseLong(ctx.toString());
                                timeStamp = new BSONTimestamp((int) (new Date(timestamp).getTime() / 1000), 1);
                            }
                        }
                    } catch (Throwable t) {
                        logger.error("Could not set initial timestamp", t);
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
                String maad = "";
                String mlu = "";
                String mlp = "";
                String mlad = "";
                // String mdu = "";
                // String mdp = "";
                Object mongoCredentialsSettings = mongoSettings.get(CREDENTIALS_FIELD);
                boolean array = XContentMapValues.isArray(mongoCredentialsSettings);

                if (array) {
                    ArrayList<Map<String, Object>> credentials = (ArrayList<Map<String, Object>>) mongoCredentialsSettings;
                    for (Map<String, Object> credential : credentials) {
                        dbCredential = XContentMapValues.nodeStringValue(credential.get(DB_FIELD), null);
                        if (ADMIN_DB_FIELD.equals(dbCredential)) {
                            mau = XContentMapValues.nodeStringValue(credential.get(USER_FIELD), null);
                            map = XContentMapValues.nodeStringValue(credential.get(PASSWORD_FIELD), null);
                            maad = XContentMapValues.nodeStringValue(credential.get(AUTH_FIELD), null);
                        } else if (LOCAL_DB_FIELD.equals(dbCredential)) {
                            mlu = XContentMapValues.nodeStringValue(credential.get(USER_FIELD), null);
                            mlp = XContentMapValues.nodeStringValue(credential.get(PASSWORD_FIELD), null);
                            mlad = XContentMapValues.nodeStringValue(credential.get(AUTH_FIELD), null);
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
                builder.mongoAdminAuthDatabase(maad);
                builder.mongoLocalUser(mlu);
                builder.mongoLocalPassword(mlp);
                builder.mongoLocalAuthDatabase(mlad);
                // mongoDbUser = mdu;
                // mongoDbPassword = mdp;
            }

            builder.mongoDb(XContentMapValues.nodeStringValue(mongoSettings.get(DB_FIELD), riverName));
            builder.mongoCollection(XContentMapValues.nodeStringValue(mongoSettings.get(COLLECTION_FIELD), riverName));
            builder.mongoGridFS(XContentMapValues.nodeBooleanValue(mongoSettings.get(GRIDFS_FIELD), false));
            if (mongoSettings.containsKey(FILTER_FIELD)) {
                String filter = XContentMapValues.nodeStringValue(mongoSettings.get(FILTER_FIELD), "");
                filter = removePrefix("o.", filter);
                builder.mongoCollectionFilter(convertToBasicDBObject(filter));
                // DBObject bsonObject = (DBObject) JSON.parse(filter);
                // builder.mongoOplogFilter(convertToBasicDBObject(addPrefix("o.",
                // filter)));
                builder.mongoOplogFilter(convertToBasicDBObject(removePrefix("o.", filter)));
                // } else {
                // builder.mongoOplogFilter("");
            }

            if (mongoSettings.containsKey(SCRIPT_FIELD)) {
                String scriptType = "js";
                builder.script(mongoSettings.get(SCRIPT_FIELD).toString());
                if (mongoSettings.containsKey("scriptType")) {
                    scriptType = mongoSettings.get("scriptType").toString();
                } else if (mongoSettings.containsKey(SCRIPT_TYPE_FIELD)) {
                    scriptType = mongoSettings.get(SCRIPT_TYPE_FIELD).toString();
                }
                builder.scriptType(scriptType);
            }
        } else {
            mongoHost = DEFAULT_DB_HOST;
            mongoPort = DEFAULT_DB_PORT;

            mongoServers.add(new ServerAddress(mongoHost, mongoPort));
            builder.mongoServers(mongoServers);
            builder.mongoDb(riverName);
            builder.mongoCollection(riverName);
        }

        if (settings.settings().containsKey(INDEX_OBJECT)) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get(INDEX_OBJECT);
            builder.indexName(XContentMapValues.nodeStringValue(indexSettings.get(NAME_FIELD), builder.mongoDb));
            builder.typeName(XContentMapValues.nodeStringValue(indexSettings.get(TYPE_FIELD), builder.mongoDb));

            Bulk.Builder bulkBuilder = new Bulk.Builder();
            if (indexSettings.containsKey(BULK_FIELD)) {
                Map<String, Object> bulkSettings = (Map<String, Object>) indexSettings.get(BULK_FIELD);
                int bulkActions = XContentMapValues.nodeIntegerValue(bulkSettings.get(ACTIONS_FIELD), DEFAULT_BULK_ACTIONS);
                bulkBuilder.bulkActions(bulkActions);
                String size = XContentMapValues.nodeStringValue(bulkSettings.get(SIZE_FIELD), DEFAULT_BULK_SIZE.toString());
                bulkBuilder.bulkSize(ByteSizeValue.parseBytesSizeValue(size));
                bulkBuilder.concurrentRequests(XContentMapValues.nodeIntegerValue(bulkSettings.get(CONCURRENT_REQUESTS_FIELD),
                        EsExecutors.boundedNumberOfProcessors(ImmutableSettings.EMPTY)));
                bulkBuilder.flushInterval(XContentMapValues.nodeTimeValue(bulkSettings.get(FLUSH_INTERVAL_FIELD), DEFAULT_FLUSH_INTERVAL));
                builder.throttleSize(XContentMapValues.nodeIntegerValue(indexSettings.get(THROTTLE_SIZE_FIELD), bulkActions * 5));
            } else {
                int bulkActions = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE_FIELD), DEFAULT_BULK_ACTIONS);
                bulkBuilder.bulkActions(bulkActions);
                bulkBuilder.bulkSize(DEFAULT_BULK_SIZE);
                bulkBuilder.flushInterval(XContentMapValues.nodeTimeValue(indexSettings.get(BULK_TIMEOUT_FIELD), DEFAULT_FLUSH_INTERVAL));
                bulkBuilder.concurrentRequests(XContentMapValues.nodeIntegerValue(indexSettings.get(CONCURRENT_BULK_REQUESTS_FIELD),
                        EsExecutors.boundedNumberOfProcessors(ImmutableSettings.EMPTY)));
                builder.throttleSize(XContentMapValues.nodeIntegerValue(indexSettings.get(THROTTLE_SIZE_FIELD), bulkActions * 5));
            }
            builder.bulk(bulkBuilder.build());
        } else {
            builder.indexName(builder.mongoDb);
            builder.typeName(builder.mongoDb);
            builder.bulk(new Bulk.Builder().build());
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
                public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }

                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }
            } };
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            sslSocketFactory = sslContext.getSocketFactory();
            return sslSocketFactory;
        } catch (Exception ex) {
            logger.warn("Unable to build ssl socket factory without certificate validation, using default instead.", ex);
        }
        return SSLSocketFactory.getDefault();
    }

    static BasicDBObject convertToBasicDBObject(String object) {
        if (object == null || object.length() == 0) {
            return new BasicDBObject();
        } else {
            return (BasicDBObject) JSON.parse(object);
        }
    }

    static String removePrefix(String prefix, String object) {
        return addRemovePrefix(prefix, object, false);
    }

    static String addPrefix(String prefix, String object) {
        return addRemovePrefix(prefix, object, true);
    }

    static String addRemovePrefix(String prefix, String object, boolean add) {
        if (prefix == null) {
            throw new IllegalArgumentException("prefix");
        }
        if (object == null) {
            throw new NullPointerException("object");
        }
        if (object.length() == 0) {
            return "";
        }
        DBObject bsonObject = (DBObject) JSON.parse(object);

        BasicBSONObject newObject = new BasicBSONObject();
        for (String key : bsonObject.keySet()) {
            if (add) {
                newObject.put(prefix + key, bsonObject.get(key));
            } else {
                if (key.startsWith(prefix)) {
                    newObject.put(key.substring(prefix.length()), bsonObject.get(key));
                } else {
                    newObject.put(key, bsonObject.get(key));
                }
            }
        }
        return newObject.toString();
    }

    private MongoDBRiverDefinition(final Builder builder) {
        // river
        this.riverName = builder.riverName;
        this.riverIndexName = builder.riverIndexName;

        // mongodb.servers
        this.mongoServers.addAll(builder.mongoServers);
        // mongodb
        this.mongoDb = builder.mongoDb;
        this.mongoCollection = builder.mongoCollection;
        this.mongoGridFS = builder.mongoGridFS;
        this.mongoOplogFilter = builder.mongoOplogFilter;
        this.mongoCollectionFilter = builder.mongoCollectionFilter;
        // mongodb.credentials
        this.mongoAdminUser = builder.mongoAdminUser;
        this.mongoAdminPassword = builder.mongoAdminPassword;
        this.mongoAdminAuthDatabase = builder.mongoAdminAuthDatabase;
        this.mongoLocalUser = builder.mongoLocalUser;
        this.mongoLocalPassword = builder.mongoLocalPassword;
        this.mongoLocalAuthDatabase = builder.mongoLocalAuthDatabase;

        // mongodb.options
        this.mongoClientOptions = builder.mongoClientOptions;
        this.connectTimeout = builder.connectTimeout;
        this.socketTimeout = builder.socketTimeout;
        this.mongoSecondaryReadPreference = builder.mongoSecondaryReadPreference;
        this.mongoUseSSL = builder.mongoUseSSL;
        this.mongoSSLVerifyCertificate = builder.mongoSSLVerifyCertificate;
        this.dropCollection = builder.dropCollection;
        this.isMongos = builder.isMongos;
        this.excludeFields = builder.excludeFields;
        this.includeFields = builder.includeFields;
        this.includeCollection = builder.includeCollection;
        this.initialTimestamp = builder.initialTimestamp;
        this.script = builder.script;
        this.scriptType = builder.scriptType;
        this.advancedTransformation = builder.advancedTransformation;
        this.skipInitialImport = builder.skipInitialImport;
        this.parentTypes = builder.parentTypes;
        this.storeStatistics = builder.storeStatistics;
        this.statisticsIndexName = builder.statisticsIndexName;
        this.statisticsTypeName = builder.statisticsTypeName;
        this.importAllCollections = builder.importAllCollections;
        this.disableIndexRefresh = builder.disableIndexRefresh;

        // index
        this.indexName = builder.indexName;
        this.typeName = builder.typeName;
        this.throttleSize = builder.throttleSize;

        // bulk
        this.bulk = builder.bulk;
    }

    public List<ServerAddress> getMongoServers() {
        return mongoServers;
    }

    public String getRiverName() {
        return riverName;
    }

    public String getRiverIndexName() {
        return riverIndexName;
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

    public BasicDBObject getMongoOplogFilter() {
        return mongoOplogFilter;
    }

    public BasicDBObject getMongoCollectionFilter() {
        return mongoCollectionFilter;
    }

    public String getMongoAdminUser() {
        return mongoAdminUser;
    }

    public String getMongoAdminPassword() {
        return mongoAdminPassword;
    }
    
    public String getMongoAdminAuthDatabase() {
        return mongoAdminAuthDatabase;
    }

    public String getMongoLocalUser() {
        return mongoLocalUser;
    }

    public String getMongoLocalPassword() {
        return mongoLocalPassword;
    }
    
    public String getMongoLocalAuthDatabase() {
        return mongoLocalAuthDatabase;
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

    public Boolean isMongos() {
        return isMongos;
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

    public Timestamp<?> getInitialTimestamp() {
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

    public boolean isSkipInitialImport() {
        return skipInitialImport;
    }

    public Set<String> getParentTypes() {
        return parentTypes;
    }

    public boolean isStoreStatistics() {
        return storeStatistics;
    }

    public String getStatisticsIndexName() {
        return statisticsIndexName;
    }

    public String getStatisticsTypeName() {
        return statisticsTypeName;
    }

    public boolean isImportAllCollections() {
        return importAllCollections;
    }

    public boolean isDisableIndexRefresh() {
        return disableIndexRefresh;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTypeName() {
        return typeName;
    }

    /*
     * Default throttle size is: 5 * bulk.bulkActions
     */
    public int getThrottleSize() {
        return throttleSize;
    }

    public String getMongoOplogNamespace() {
        return getMongoDb() + "." + getMongoCollection();
    }

    public Bulk getBulk() {
        return bulk;
    }
}
