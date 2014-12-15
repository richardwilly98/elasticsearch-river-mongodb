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

import static com.google.common.collect.ObjectArrays.concat;
import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.PluginManager;
import org.elasticsearch.plugins.PluginManager.OutputMode;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.mongodb.embed.TokuMXStarter;
import org.elasticsearch.river.mongodb.embed.TokuRuntimeConfigBuilder;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.script.ScriptService;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Versions;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.distribution.BitSize;
import de.flapdoodle.embed.process.distribution.GenericVersion;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.runtime.Network;
import de.flapdoodle.embed.process.runtime.Starter;

public abstract class RiverMongoDBTestAbstract {

    public static class MongoReplicaSet {

        static class Member {
            private IMongodConfig config;
            private MongodExecutable executable;
            private MongodProcess process;
            private ServerAddress address;
        }

        private final ExecutableType type;
        private final String version;
        public final Mongo mongo;
        private final DB mongoAdminDB;
        private final ImmutableList<Member> members;

        public MongoReplicaSet(ExecutableType type, String version, Mongo mongo, DB mongoAdminDB, ImmutableList<Member> members) {
            this.type = type;
            this.version = version;
            this.mongo = mongo;
            this.mongoAdminDB = mongoAdminDB;
            this.members = members;
        }
    }

    public static enum ExecutableType {
        VANILLA("mongodb", true, true) {
            @Override
            public Starter<IMongodConfig, MongodExecutable, MongodProcess> getStarter() {
                return MongodStarter.getInstance(getRuntimeConfig());
            }

            @Override
            public RuntimeConfigBuilder getRuntimeConfigBuilder() {
                return new RuntimeConfigBuilder();
            }
        },
        TOKUMX("tokumx", tokuIsSupported(), false) {
            @Override
            public Starter<IMongodConfig, MongodExecutable, MongodProcess> getStarter() {
                return TokuMXStarter.getInstance(getRuntimeConfig());
            }

            @Override
            public RuntimeConfigBuilder getRuntimeConfigBuilder() {
                return new TokuRuntimeConfigBuilder();
            }
        };

        public final String configKey;
        public final boolean isSupported;
        public final boolean supportsGridFS;

        private ExecutableType(String configKey, boolean isSupported, boolean supportsGridFS) {
            this.configKey = configKey;
            this.supportsGridFS = supportsGridFS;
            this.isSupported = isSupported;
        }

        public abstract Starter<IMongodConfig, MongodExecutable, MongodProcess> getStarter();

        protected abstract RuntimeConfigBuilder getRuntimeConfigBuilder();

        protected IRuntimeConfig getRuntimeConfig() {
            return getRuntimeConfigBuilder()
                    .defaults(Command.MongoD)
                    .processOutput(ProcessOutput.getDefaultInstance(configKey))
                    .build();
        }
    }

    public static final String TEST_MONGODB_RIVER_SIMPLE_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river.json";
    public static final String TEST_MONGODB_RIVER_SIMPLE_WITH_TYPE_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-with-type.json";
    public static final String TEST_MONGODB_RIVER_GRIDFS_JSON = "/org/elasticsearch/river/mongodb/gridfs/test-gridfs-mongodb-river.json";
    public static final String TEST_MONGODB_RIVER_WITH_SCRIPT_JSON = "/org/elasticsearch/river/mongodb/script/test-mongodb-river-with-script.json";
    public static final String TEST_MONGODB_RIVER_EXCLUDE_FIELDS_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-exclude-fields.json";
    public static final String TEST_MONGODB_RIVER_INCLUDE_FIELDS_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-include-fields.json";
    public static final String TEST_MONGODB_RIVER_IMPORT_ALL_COLLECTION_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-import-all-collections.json";
    public static final String TEST_MONGODB_RIVER_STORE_STATISTICS_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-store-statistics.json";
    public static final String TEST_SIMPLE_MONGODB_DOCUMENT_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-document.json";

    protected final ESLogger logger = Loggers.getLogger(getClass().getName());
    protected final static long wait = 2000;

    public static final String ADMIN_DATABASE_NAME = "admin";
    public static final String LOCAL_DATABASE_NAME = "local";
    //public static final String REPLICA_SET_NAME = "rep1";
    public static final String OPLOG_COLLECTION = "oplog.rs";

    private static Settings settings = loadSettings();
    private static EnumMap<ExecutableType, MongoReplicaSet> replicaSets =
            new EnumMap<RiverMongoDBTestAbstract.ExecutableType, RiverMongoDBTestAbstract.MongoReplicaSet>(ExecutableType.class);
    private static Node node;

    protected final ExecutableType executableType;
    private final String river;
    private final String database;
    private final String collection;
    private final String index;

    protected RiverMongoDBTestAbstract(boolean isGridFS) {
        this(ExecutableType.VANILLA, isGridFS);
    }

    protected RiverMongoDBTestAbstract(ExecutableType type) {
        this(type, false);
    }

    protected RiverMongoDBTestAbstract(ExecutableType type, boolean isGridFS) {
        Validate.isTrue(type.supportsGridFS || !isGridFS, "isGridFS is not supported by " + type);
        this.executableType = type;
        String suffix = getClass().getSimpleName().toLowerCase();
        if (suffix.length() > 35) {
            suffix = suffix.substring(0, 34);
        }
        suffix = suffix + "-" + System.currentTimeMillis();
        this.river = "r" + type.ordinal() + "-" + suffix;
        this.database = "d-" + suffix;
        if (isGridFS) {
            this.collection = "fs";
        } else {
            this.collection = "c-" + suffix;
        }
        this.index = "i" + type.ordinal() + "-" + suffix;
    }

    private static Iterable<ExecutableType> supportedExecutableTypes() {
        return Iterables.filter(Arrays.asList(ExecutableType.values()), new Predicate<ExecutableType>() {
            @Override public boolean apply(ExecutableType _) { return _.isSupported; }});
    }

    protected static boolean tokuIsSupported() {
        return Platform.detect() == Platform.Linux && BitSize.detect() == BitSize.B64;
    }

    /** Only include TOKUMX if on a supported platform */
    @DataProvider(name = "allMongoExecutableTypes")
    public static Object[][] allMongoExecutableTypes() {
        return Iterables.toArray(Iterables.transform(supportedExecutableTypes(), new Function<ExecutableType, Object[]>() {
            @Override public Object[] apply(ExecutableType _) { return new Object[] { _ }; }}),
            Object[].class);
    }

    @DataProvider(name = "onlyVanillaMongo")
    public static Object[][] onlyVanillaMongo() {
        return new Object[][] {{ ExecutableType.VANILLA }};
    }

    @BeforeSuite
    public void beforeSuite() throws Exception {
        logger.debug("*** beforeSuite ***");
        setupElasticsearchServer();
        initMongoInstances();
    }

    private static Settings loadSettings() {
        return settingsBuilder().loadFromStream("settings.yml", ClassLoader.getSystemResourceAsStream("settings.yml")).build();
    }

    private void initMongoInstances() throws Exception {
        for (ExecutableType type : supportedExecutableTypes()) {
            initMongoInstances(type);
        }
    }

    private void initMongoInstances(ExecutableType type) throws Exception {
        logger.debug("*** initMongoInstances(" + type + ") ***");
        CommandResult cr;
        Settings rsSettings = settings.getByPrefix(type.configKey + '.');
        int[] ports;
        if (rsSettings.getAsBoolean("useDynamicPorts", false)) {
            ports = new int[] { Network.getFreeServerPort(), Network.getFreeServerPort(), Network.getFreeServerPort() };
        } else {
            int start = 37017 + 10 * type.ordinal();
            ports = new int[] { start, start + 1, start + 2 };
        }
        String replicaSetName = "es-test-" + type.configKey;
        // Create 3 mongod processes
        Starter<IMongodConfig, MongodExecutable, MongodProcess> starter = type.getStarter();
        ImmutableList.Builder<MongoReplicaSet.Member> builder = ImmutableList.builder();
        for (int i = 1; i <= 3; ++i) {
            Storage storage = new Storage("target/" + replicaSetName + '/' + i, replicaSetName, 20);
            MongoReplicaSet.Member member = new MongoReplicaSet.Member();
            member.config = new MongodConfigBuilder().version(Versions.withFeatures(new GenericVersion(rsSettings.get("version"))))
                .net(new de.flapdoodle.embed.mongo.config.Net(ports[i - 1], Network.localhostIsIPv6())).replication(storage).build();
            logger.trace("replSetName in config: {}", member.config.replication().getReplSetName());
            member.executable = starter.prepare(member.config);
            member.process = member.executable.start();
            member.address = new ServerAddress(Network.getLocalHost().getHostName(), member.config.net().getPort());
            logger.debug("Server #" + i + ": {}", member.address);
            builder.add(member);
        }
        ImmutableList<MongoReplicaSet.Member> members = builder.build();
        Thread.sleep(2000);
        MongoClientOptions mco = MongoClientOptions.builder().autoConnectRetry(true).connectTimeout(15000).socketTimeout(60000).build();
        Mongo mongo = new MongoClient(new ServerAddress(Network.getLocalHost().getHostName(), ports[0]), mco);
        DB mongoAdminDB = mongo.getDB(ADMIN_DATABASE_NAME);

        cr = mongoAdminDB.command(new BasicDBObject("isMaster", 1));
        logger.debug("isMaster: " + cr);

        // Initialize replica set
        cr = mongoAdminDB.command(new BasicDBObject("replSetInitiate",
                (DBObject) JSON.parse("{'_id': '" + replicaSetName + "', 'members': ["
                + "{'_id': 0, 'host': '" + members.get(0).address.getHost() + ':' + members.get(0).address.getPort() + "'}, "
                + "{'_id': 1, 'host': '" + members.get(1).address.getHost() + ':' + members.get(1).address.getPort() + "'}, "
                + "{'_id': 2, 'host': '" + members.get(2).address.getHost() + ':' + members.get(2).address.getPort() + "', 'arbiterOnly' : true}]} }")));
        logger.debug("replSetInitiate result: " + cr);

        Thread.sleep(5000);
        cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
        logger.trace("replSetGetStatus result: {}", cr);

        // Check replica set status before to proceed
        while (!isReplicaSetStarted(cr)) {
            logger.debug("Waiting 3 seconds for replicaset to change status...");
            Thread.sleep(3000);
            cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
            // logger.debug("replSetGetStatus: " + cr);
        }

        mongo.close();
        mongo = null;

        // Initialize a new client using all instances.
        List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
        for (MongoReplicaSet.Member member : members) {
            mongoServers.add(member.address);
        }
        mongo = new MongoClient(mongoServers, mco);
        Assert.assertNotNull(mongo);
        mongo.setReadPreference(ReadPreference.secondaryPreferred());
        mongo.setWriteConcern(WriteConcern.REPLICAS_SAFE);
        replicaSets.put(type, new MongoReplicaSet(type, rsSettings.get("version"), mongo, mongoAdminDB, members));
    }

    private boolean isReplicaSetStarted(BasicDBObject setting) {
        if (setting.get("members") == null) {
            return false;
        }

        BasicDBList members = (BasicDBList) setting.get("members");
        int numPrimaries = 0;
        for (Object m : members.toArray()) {
            BasicDBObject member = (BasicDBObject) m;
            logger.trace("Member: {}", member);
            int state = member.getInt("state");
            logger.info("Member state: " + state);
            // 1 - PRIMARY, 2 - SECONDARY, 7 - ARBITER
            if (state != 1 && state != 2 && state != 7) {
                return false;
            }
            if (state == 1) {
                ++numPrimaries;
            }
        }
        if (numPrimaries != 1) {
            logger.warn("Expected 1 primary, instead found " + numPrimaries);
            return false;
        }
        return true;
    }

    private void setupElasticsearchServer() throws Exception {
        logger.debug("*** setupElasticsearchServer ***");
        try {
            Tuple<Settings, Environment> initialSettings = InternalSettingsPreparer.prepareSettings(settings, true);
            if (!initialSettings.v2().configFile().exists()) {
                FileSystemUtils.mkdirs(initialSettings.v2().configFile());
            }

            if (!initialSettings.v2().logsFile().exists()) {
                FileSystemUtils.mkdirs(initialSettings.v2().logsFile());
            }

            if (!initialSettings.v2().pluginsFile().exists()) {
                FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
                if (settings.getByPrefix("plugins") != null) {
                    PluginManager pluginManager = new PluginManager(initialSettings.v2(), null, OutputMode.DEFAULT, PluginManager.DEFAULT_TIMEOUT);

                    Map<String, String> plugins = settings.getByPrefix("plugins").getAsMap();
                    for (String key : plugins.keySet()) {
                        pluginManager.downloadAndExtract(plugins.get(key));
                    }
                }
            } else {
                logger.info("Plugin {} has been already installed.", settings.get("plugins.mapper-attachments"));
                logger.info("Plugin {} has been already installed.", settings.get("plugins.lang-javascript"));
            }

            node = nodeBuilder().local(true).settings(settings).node();
        } catch (Exception ex) {
            logger.error("setupElasticsearchServer failed", ex);
            throw ex;
        }
    }

    protected String getJsonSettings(String jsonDefinition, int numPortArgs, Object... additionalArgs) throws Exception {
        logger.debug("Get river setting");
        String setting = copyToStringFromClasspath(jsonDefinition);
        switch(numPortArgs) {
        case 0:
            return additionalArgs == null ? setting : String.format(setting, additionalArgs);
        case 1:
            return String.format(setting, concat(String.valueOf(getMongoPort(1)), additionalArgs));
        case 3:
            List<String> ports = Arrays.asList(
                    String.valueOf(getMongoPort(1)), String.valueOf(getMongoPort(2)), String.valueOf(getMongoPort(3)));
            return String.format(setting, concat(ports.toArray(), additionalArgs, Object.class));
        default:
            throw new IllegalArgumentException("numPortArgs must be one of { 0, 1, 3 }");
        }
    }

    protected void refreshIndex() {
        refreshIndex(index);
    }

    protected void refreshIndex(String index) {
        getNode().client().admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

    protected void waitForGreenStatus() {
        try {
            logger.debug("Running Cluster Health");
            logger.info("Done Cluster Health, status {}",
                    node.client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).get().getStatus());
        } catch (Exception ex) {
            Assert.fail("waitForGreenStatus failed", ex);
        }

    }

    /** Prepend MongoDB ports as first numPortArgs to format jsonDefinition with. */
    protected void createRiver(String jsonDefinition, String river, int numPortArgs, Object... args) throws Exception {
        logger.info("Create river [{}]", river);
        String settings = getJsonSettings(jsonDefinition, numPortArgs, args);
        logger.info("River setting [{}]", settings);
        node.client().prepareIndex("_river", river, "_meta").setSource(settings).execute().actionGet();
        waitForGreenStatus();
        GetResponse response = getNode().client().prepareGet("_river", river, "_meta").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        int count = 0;
        while (true) {
            refreshIndex("_river");
            if (MongoDBRiverHelper.getRiverStatus(node.client(), river) != Status.UNKNOWN) {
                break;
            } else {
                logger.debug("Wait for river [{}] to start", river);
            }
            if (count == 5) {
                throw new Exception(String.format("Fail to create and start river %s", river));
            }
            Thread.sleep(1000);
            count++;
        }
    }

    protected void createRiver(String jsonDefinition) throws Exception {
        createRiver(jsonDefinition, getDatabase(), getCollection(), getIndex());
    }

    protected void createRiver(String jsonDefinition, String database, String collection, String index) throws Exception {
        createRiver(jsonDefinition, river, 3, database, collection, index);
    }

    protected void deleteIndex(String name) {
        int max = 5;
        int count = 0;
        logger.info("Delete index [{}]", name);
        IndicesExistsResponse response = node.client().admin().indices().prepareExists(name).get();
        if (!response.isExists()) {
            logger.info("Index {} does not exist", name);
            return;
        }
        if (!node.client().admin().indices().prepareDelete(name).execute().actionGet().isAcknowledged()) {
            response = node.client().admin().indices().prepareExists(name).get();
            while (response.isExists()) {
                logger.debug("Index {} not deleted. Try waiting 1 sec...", name);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                response = node.client().admin().indices().prepareExists(name).get();
                count++;
                if (count == max) {
                    Assert.fail(String.format("Could not delete index %s", name));
                }
            }
        }
        waitForGreenStatus();
    }

    protected void deleteIndex() {
        deleteIndex(index);
    }

    protected void deleteRiver() {
        deleteRiver(river);
    }

    protected void deleteRiver(String name) {
        try {
            int max = 5;
            int count = 0;
            logger.info("Delete river [{}]", name);
            // if
            // (!node.client().admin().indices().prepareDeleteMapping("_river").setType(name).get().isAcknowledged())
            // {
            node.client().admin().indices().prepareDeleteMapping("_river").setType(name).get();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            refreshIndex("_river");
            TypesExistsResponse response = node.client().admin().indices().prepareTypesExists("_river").setTypes(name).get();
            while (response.isExists()) {
                logger.debug("River {} not deleted. Try waiting 1 sec...", name);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                node.client().admin().indices().prepareDeleteMapping("_river").setType(name).get();
                refreshIndex("_river");
                response = node.client().admin().indices().prepareTypesExists("_river").setTypes(name).get();
                count++;
                if (count == max) {
                    Assert.fail(String.format("Could not delete river %s", name));
                }
            }
            waitForGreenStatus();
        } catch (Throwable t) {
            logger.error("Delete river [{}] failed", t, name);
        }
    }

    protected MongoDBRiverDefinition getMongoDBRiverDefinition(String jsonDefinition, String database, String collection, String index)
            throws Throwable {
        try {
            RiverName riverName = new RiverName("mongodb", river);
            // InputStream in =
            // getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-simple-definition.json");
            String settings = getJsonSettings(jsonDefinition, 3, database, collection, index);
            InputStream in = new ByteArrayInputStream(settings.getBytes());
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(),
                    RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverSettings, scriptService);
            Assert.assertNotNull(definition);
            return definition;
        } catch (Throwable t) {
            Assert.fail("testLoadMongoDBRiverSimpleDefinition failed", t);
            throw t;
        }
    }

    @AfterSuite
    public void afterSuite() {
        logger.debug("*** afterSuite ***");
        shutdownElasticsearchServer();
        shutdownMongoInstances();
    }

    private void shutdownMongoInstances() {
        logger.debug("*** shutdownMongoInstances ***");
        for (MongoReplicaSet rs : replicaSets.values()) {
            shutdownMongoInstances(rs);
        }
    }

    private void shutdownMongoInstances(MongoReplicaSet rs) {
        rs.mongo.close();
        for (MongoReplicaSet.Member member : rs.members) {
            try {
                logger.debug("Start shutdown {}", member);
                member.process.stop();
            } catch (Throwable t) {
            }
        }
    }

    private void shutdownElasticsearchServer() {
        logger.debug("*** shutdownElasticsearchServer ***");
        node.close();
    }

    protected Mongo getMongo() {
        return replicaSets.get(executableType).mongo;
    }

    protected static Node getNode() {
        return node;
    }

    protected static IndicesAdminClient getIndicesAdminClient() {
        return node.client().admin().indices();
    }

    private int getMongoPort(int n) {
        return replicaSets.get(executableType).members.get(n - 1).address.getPort();
    }

    protected String getRiver() {
        return river;
    }

    protected String getDatabase() {
        return database;
    }

    protected String getCollection() {
        return collection;
    }

    protected String getIndex() {
        return index;
    }

    /** Print a more useful string for each instance, in TestNG reports. */
    @Override
    public String toString() {
        return executableType.name();
    }
}
