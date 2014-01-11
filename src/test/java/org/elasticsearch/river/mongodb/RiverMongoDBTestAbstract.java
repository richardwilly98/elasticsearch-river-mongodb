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

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.script.ScriptService;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

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

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Versions;
import de.flapdoodle.embed.process.distribution.GenericVersion;
import de.flapdoodle.embed.process.runtime.Network;

public abstract class RiverMongoDBTestAbstract {

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
    public static final String REPLICA_SET_NAME = "rep1";
    public static final String OPLOG_COLLECTION = "oplog.rs";

    private IMongodConfig mongodConfig1;
    private IMongodConfig mongodConfig2;
    private IMongodConfig mongodConfig3;
    private MongodExecutable mongodExe1;
    private static int mongoPort1;
    private static MongodProcess mongod1;
    private MongodExecutable mongodExe2;
    private static int mongoPort2;
    private static MongodProcess mongod2;
    private MongodExecutable mongodExe3;
    private static int mongoPort3;
    private static MongodProcess mongod3;
    protected static Mongo mongo;
    private DB mongoAdminDB;

    private static Node node;
    private static Settings settings;

    private boolean useDynamicPorts;
    private String mongoVersion;

    private final String river;
    private final String database;
    private final String collection;
    private final String index;

    protected RiverMongoDBTestAbstract(/*
                                        * String river, String database, String
                                        * collection, String index
                                        */) {
        // this.river = river;
        // this.database = database;
        // this.collection = collection;
        // this.index = index;
        this(false);
    }

    protected RiverMongoDBTestAbstract(boolean isGridFS) {
        String suffix = getClass().getSimpleName().toLowerCase();
        if (suffix.length() > 35) {
            suffix = suffix.substring(0, 34);
        }
        suffix = suffix + "-" + System.currentTimeMillis();
        this.river = "r-" + suffix;
        this.database = "d-" + suffix;
        if (isGridFS) {
            this.collection = "fs";
        } else {
            this.collection = "c-" + suffix;
        }
        this.index = "i-" + suffix;
        loadSettings();
    }

    @BeforeSuite
    public void beforeSuite() throws Exception {
        logger.debug("*** beforeSuite ***");
        if (useDynamicPorts) {
            mongoPort1 = Network.getFreeServerPort();
            mongoPort2 = Network.getFreeServerPort();
            mongoPort3 = Network.getFreeServerPort();
        } else {
            mongoPort1 = 37017;
            mongoPort2 = 37018;
            mongoPort3 = 37019;
        }
        setupElasticsearchServer();
        initMongoInstances();
    }

    private void loadSettings() {
        settings = settingsBuilder().loadFromStream("settings.yml", ClassLoader.getSystemResourceAsStream("settings.yml")).build();

        this.useDynamicPorts = settings.getAsBoolean("mongodb.use_dynamic_ports", Boolean.FALSE);
        this.mongoVersion = settings.get("mongodb.version");
    }

    private void initMongoInstances() throws Exception {
        logger.debug("*** initMongoInstances ***");
        CommandResult cr;

        // Create 3 mongod processes
        MongodStarter starter = MongodStarter.getDefaultInstance();
        Storage storage1 = new Storage("target/mongodb/1", REPLICA_SET_NAME, 20);
        Storage storage2 = new Storage("target/mongodb/2", REPLICA_SET_NAME, 20);
        Storage storage3 = new Storage("target/mongodb/3", REPLICA_SET_NAME, 20);

        mongodConfig1 = new MongodConfigBuilder().version(Versions.withFeatures(new GenericVersion(mongoVersion)))
                .net(new de.flapdoodle.embed.mongo.config.Net(mongoPort1, Network.localhostIsIPv6())).replication(storage1).build();
        mongodExe1 = starter.prepare(mongodConfig1);
        mongod1 = mongodExe1.start();

        mongodConfig2 = new MongodConfigBuilder().version(Versions.withFeatures(new GenericVersion(mongoVersion)))
                .net(new de.flapdoodle.embed.mongo.config.Net(mongoPort2, Network.localhostIsIPv6())).replication(storage2).build();
        mongodExe2 = starter.prepare(mongodConfig2);
        mongod2 = mongodExe2.start();

        mongodConfig3 = new MongodConfigBuilder().version(Versions.withFeatures(new GenericVersion(mongoVersion)))
                .net(new de.flapdoodle.embed.mongo.config.Net(mongoPort3, Network.localhostIsIPv6())).replication(storage3).build();
        mongodExe3 = starter.prepare(mongodConfig3);
        mongod3 = mongodExe3.start();
        String server1 = Network.getLocalHost().getHostName() + ":" + mongodConfig1.net().getPort();
        String server2 = Network.getLocalHost().getHostName() + ":" + mongodConfig2.net().getPort();
        String server3 = Network.getLocalHost().getHostName() + ":" + mongodConfig3.net().getPort();
        logger.debug("Server #1: {}", server1);
        logger.debug("Server #2: {}", server2);
        logger.debug("Server #3: {}", server3);
        Thread.sleep(2000);
        MongoClientOptions mco = MongoClientOptions.builder().autoConnectRetry(true).connectTimeout(15000).socketTimeout(60000).build();
        mongo = new MongoClient(new ServerAddress(Network.getLocalHost().getHostName(), mongodConfig1.net().getPort()), mco);
        mongoAdminDB = mongo.getDB(ADMIN_DATABASE_NAME);

        cr = mongoAdminDB.command(new BasicDBObject("isMaster", 1));
        logger.debug("isMaster: " + cr);

        // Initialize replica set
        cr = mongoAdminDB.command(new BasicDBObject("replSetInitiate", (DBObject) JSON.parse("{'_id': '" + REPLICA_SET_NAME
                + "', 'members': [{'_id': 0, 'host': '" + server1 + "'}, {'_id': 1, 'host': '" + server2 + "'}, {'_id': 2, 'host': '"
                + server3 + "', 'arbiterOnly' : true}]} }")));
        logger.debug("replSetInitiate: " + cr);

        Thread.sleep(5000);
        cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
        logger.trace("replSetGetStatus: {}", cr);

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
        mongoServers.add(new ServerAddress(Network.getLocalHost().getHostName(), mongodConfig1.net().getPort()));
        mongoServers.add(new ServerAddress(Network.getLocalHost().getHostName(), mongodConfig2.net().getPort()));
        mongoServers.add(new ServerAddress(Network.getLocalHost().getHostName(), mongodConfig3.net().getPort()));
        mongo = new MongoClient(mongoServers, mco);
        Assert.assertNotNull(mongo);
        mongo.setReadPreference(ReadPreference.secondaryPreferred());
        mongo.setWriteConcern(WriteConcern.REPLICAS_SAFE);
    }

    private boolean isReplicaSetStarted(BasicDBObject setting) {
        if (setting.get("members") == null) {
            return false;
        }

        BasicDBList members = (BasicDBList) setting.get("members");
        for (Object m : members.toArray()) {
            BasicDBObject member = (BasicDBObject) m;
            logger.trace("Member: {}", member);
            int state = member.getInt("state");
            logger.info("Member state: " + state);
            // 1 - PRIMARY, 2 - SECONDARY, 7 - ARBITER
            if (state != 1 && state != 2 && state != 7) {
                return false;
            }
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

    protected String getJsonSettings(String jsonDefinition, Object... args) throws Exception {
        logger.debug("Get river setting");
        String setting = copyToStringFromClasspath(jsonDefinition);
        if (args != null) {
            setting = String.format(setting, args);
        }
        return setting;
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

    protected void createRiver(String jsonDefinition, String river, Object... args) throws Exception {
        logger.info("Create river [{}]", river);
        String settings = getJsonSettings(jsonDefinition, args);
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

    protected void createRiver(String jsonDefinition, Object... args) throws Exception {
        createRiver(jsonDefinition, river, args);
    }

    protected void createRiver(String jsonDefinition) throws Exception {
        createRiver(jsonDefinition, database, collection, index);
    }

    protected void createRiver(String jsonDefinition, String database, String collection, String index) throws Exception {
        createRiver(jsonDefinition, river, String.valueOf(getMongoPort1()), String.valueOf(getMongoPort2()),
                String.valueOf(getMongoPort3()), database, collection, index);
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
            String settings = getJsonSettings(jsonDefinition, String.valueOf(getMongoPort1()), String.valueOf(getMongoPort2()),
                    String.valueOf(getMongoPort3()), database, collection, index);
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
        mongo.close();
        try {
            logger.debug("Start shutdown {}", mongod1);
            mongod1.stop();
        } catch (Throwable t) {
        }
        try {
            logger.debug("Start shutdown {}", mongod2);
            mongod2.stop();
        } catch (Throwable t) {
        }
        try {
            logger.debug("Start shutdown {}", mongod3);
            mongod3.stop();
        } catch (Throwable t) {
        }
    }

    private void shutdownElasticsearchServer() {
        logger.debug("*** shutdownElasticsearchServer ***");
        node.close();
    }

    protected static Mongo getMongo() {
        return mongo;
    }

    protected static Node getNode() {
        return node;
    }

    protected static IndicesAdminClient getIndicesAdminClient() {
        return node.client().admin().indices();
    }

    protected static int getMongoPort1() {
        return mongoPort1;
    }

    protected static int getMongoPort2() {
        return mongoPort2;
    }

    protected static int getMongoPort3() {
        return mongoPort3;
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
}
