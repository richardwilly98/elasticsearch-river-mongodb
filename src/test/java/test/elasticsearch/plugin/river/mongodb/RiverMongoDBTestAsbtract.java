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
package test.elasticsearch.plugin.river.mongodb;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPerparer;
import org.elasticsearch.plugins.PluginManager;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

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
import de.flapdoodle.embed.process.distribution.GenericVersion;
import de.flapdoodle.embed.process.runtime.Network;

@Test
public abstract class RiverMongoDBTestAsbtract {

	public static final String TEST_MONGODB_RIVER_SIMPLE_JSON = "/test/elasticsearch/plugin/river/mongodb/simple/test-simple-mongodb-river.json";
	public static final String TEST_MONGODB_RIVER_SIMPLE_WITH_TYPE_JSON = "/test/elasticsearch/plugin/river/mongodb/simple/test-simple-mongodb-river-with-type.json";
	public static final String TEST_MONGODB_RIVER_GRIDFS_JSON = "/test/elasticsearch/plugin/river/mongodb/gridfs/test-gridfs-mongodb-river.json";
	public static final String TEST_MONGODB_RIVER_WITH_SCRIPT_JSON = "/test/elasticsearch/plugin/river/mongodb/script/test-mongodb-river-with-script.json";
	public static final String TEST_MONGODB_RIVER_EXCLUDE_FIELDS_JSON = "/test/elasticsearch/plugin/river/mongodb/simple/test-simple-mongodb-river-exclude-fields.json";
	public static final String TEST_SIMPLE_MONGODB_DOCUMENT_JSON = "/test/elasticsearch/plugin/river/mongodb/script/test-simple-mongodb-document.json";

	protected final ESLogger logger = Loggers.getLogger(getClass());
	protected final static long wait = 6000;

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
	private static Mongo mongo;
	private DB mongoAdminDB;

	private static Node node;
	private static Settings settings;

	private boolean useDynamicPorts;
	private String mongoVersion;

	private final String river;
	private final String database;
	private final String collection;
	private final String index;

	protected RiverMongoDBTestAsbtract(String river, String database,
			String collection, String index) {
		this.river = river;
		this.database = database;
		this.collection = collection;
		this.index = index;
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
		settings = settingsBuilder().loadFromStream("settings.yml",
				ClassLoader.getSystemResourceAsStream("settings.yml")).build();

		this.useDynamicPorts = settings.getAsBoolean(
				"mongodb.use_dynamic_ports", Boolean.FALSE);
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

		mongodConfig1 = new MongodConfigBuilder()
				.version(new GenericVersion(mongoVersion))
				.net(new de.flapdoodle.embed.mongo.config.Net(mongoPort1,
						Network.localhostIsIPv6())).replication(storage1)
				.build();
		mongodExe1 = starter.prepare(mongodConfig1);
		mongod1 = mongodExe1.start();

		mongodConfig2 = new MongodConfigBuilder()
				.version(new GenericVersion(mongoVersion))
				.net(new de.flapdoodle.embed.mongo.config.Net(mongoPort2,
						Network.localhostIsIPv6())).replication(storage2)
				.build();
		mongodExe2 = starter.prepare(mongodConfig2);
		mongod2 = mongodExe2.start();

		mongodConfig3 = new MongodConfigBuilder()
				.version(new GenericVersion(mongoVersion))
				.net(new de.flapdoodle.embed.mongo.config.Net(mongoPort3,
						Network.localhostIsIPv6())).replication(storage3)
				.build();
		mongodExe3 = starter.prepare(mongodConfig3);
		mongod3 = mongodExe3.start();
		String server1 = Network.getLocalHost().getHostName() + ":"
				+ mongodConfig1.net().getPort();
		String server2 = Network.getLocalHost().getHostName() + ":"
				+ mongodConfig2.net().getPort();
		String server3 = Network.getLocalHost().getHostName() + ":"
				+ mongodConfig3.net().getPort();
		logger.debug("Server #1: {}", server1);
		logger.debug("Server #2: {}", server2);
		logger.debug("Server #3: {}", server3);
		Thread.sleep(2000);
		MongoClientOptions mco = MongoClientOptions.builder()
				.autoConnectRetry(true).connectTimeout(15000)
				.socketTimeout(60000).build();
		mongo = new MongoClient(new ServerAddress(Network.getLocalHost()
				.getHostName(), mongodConfig1.net().getPort()), mco);
		mongoAdminDB = mongo.getDB(ADMIN_DATABASE_NAME);

		cr = mongoAdminDB.command(new BasicDBObject("isMaster", 1));
		logger.debug("isMaster: " + cr);

		// Initialize replica set
		cr = mongoAdminDB.command(new BasicDBObject("replSetInitiate",
				(DBObject) JSON.parse("{'_id': '" + REPLICA_SET_NAME
						+ "', 'members': [{'_id': 0, 'host': '" + server1
						+ "'}, {'_id': 1, 'host': '" + server2
						+ "'}, {'_id': 2, 'host': '" + server3
						+ "', 'arbiterOnly' : true}]} }")));
		logger.debug("replSetInitiate: " + cr);

		Thread.sleep(5000);
		cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
		logger.info("replSetGetStatus: " + cr);

		// Check replica set status before to proceed
		while (!isReplicaSetStarted(cr)) {
			logger.debug("Waiting for 3 seconds...");
			Thread.sleep(3000);
			cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
			logger.debug("replSetGetStatus: " + cr);
		}

		mongo.close();
		mongo = null;

		// Initialize a new client using all instances.
		List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
		mongoServers.add(new ServerAddress(
				Network.getLocalHost().getHostName(), mongodConfig1.net()
						.getPort()));
		mongoServers.add(new ServerAddress(
				Network.getLocalHost().getHostName(), mongodConfig2.net()
						.getPort()));
		mongoServers.add(new ServerAddress(
				Network.getLocalHost().getHostName(), mongodConfig3.net()
						.getPort()));
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
			logger.info(member.toString());
			int state = member.getInt("state");
			logger.info("state: " + state);
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
			Tuple<Settings, Environment> initialSettings = InternalSettingsPerparer
					.prepareSettings(settings, true);
			if (!initialSettings.v2().configFile().exists()) {
				FileSystemUtils.mkdirs(initialSettings.v2().configFile());
			}

			if (!initialSettings.v2().logsFile().exists()) {
				FileSystemUtils.mkdirs(initialSettings.v2().logsFile());
			}

			if (!initialSettings.v2().pluginsFile().exists()) {
				FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
				if (settings.getByPrefix("plugins") != null) {
					PluginManager pluginManager = new PluginManager(
							initialSettings.v2(), null);

					Map<String, String> plugins = settings.getByPrefix(
							"plugins").getAsMap();
					for (String key : plugins.keySet()) {
						pluginManager.downloadAndExtract(plugins.get(key),
								false);
					}
				}
			} else {
				logger.info("Plugin {} has been already installed.",
						settings.get("plugins.mapper-attachments"));
				logger.info("Plugin {} has been already installed.",
						settings.get("plugins.lang-javascript"));
			}

			node = nodeBuilder().local(true).settings(settings).node();
		} catch (Exception ex) {
			logger.error("setupElasticsearchServer failed", ex);
			throw ex;
		}
	}

	protected String getJsonSettings(String jsonDefinition, Object... args)
			throws Exception {
		logger.debug("Get river setting");
		String setting = copyToStringFromClasspath(jsonDefinition);
		if (args != null) {
			setting = String.format(setting, args);
		}
		logger.debug("River setting: {}", setting);
		return setting;
	}

	protected void refreshIndex() {
		refreshIndex(index);
	}

	protected void refreshIndex(String index) {
		getNode().client().admin().indices().refresh(new RefreshRequest(index))
				.actionGet();
	}

	protected void createRiver(String jsonDefinition, String river,
			Object... args) throws Exception {
		logger.info("Create river [{}]", river);
		String setting = getJsonSettings(jsonDefinition, args);
		logger.info("River setting [{}]", setting);
		node.client().prepareIndex("_river", river, "_meta").setSource(setting)
				.execute().actionGet();
		logger.debug("Running Cluster Health");
		ClusterHealthResponse clusterHealth = node.client().admin().cluster()
				.health(clusterHealthRequest().waitForGreenStatus())
				.actionGet();
		logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
		GetResponse response = getNode().client()
				.prepareGet("_river", river, "_meta").execute().actionGet();
		assertThat(response.isExists(), equalTo(true));
		refreshIndex("_river");
	}

	protected void createRiver(String jsonDefinition, Object... args)
			throws Exception {
		createRiver(jsonDefinition, river, args);
	}

	protected void createRiver(String jsonDefinition) throws Exception {
		createRiver(jsonDefinition, database, collection, index);
	}

	protected void createRiver(String jsonDefinition, String database,
			String collection, String index) throws Exception {
		createRiver(jsonDefinition, river, String.valueOf(getMongoPort1()),
				String.valueOf(getMongoPort2()),
				String.valueOf(getMongoPort3()), database, collection, index);
	}

	protected void deleteIndex(String name) {
		logger.info("Delete index [{}]", name);
		if (!node.client().admin().indices().prepareDelete(name).execute()
				.actionGet().isAcknowledged()) {
			logger.error("Counld not delete index: {}. Try waiting 1 sec...",
					name);
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.debug("Running Cluster Health");
		ClusterHealthResponse clusterHealth = node.client().admin().cluster()
				.health(clusterHealthRequest().waitForGreenStatus())
				.actionGet();
		logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
	}

	protected void deleteIndex() {
		deleteIndex(index);
	}

	protected void deleteRiver() {
		deleteRiver(river);
	}

	protected void deleteRiver(String name) {
		logger.info("Delete river [{}]", name);
		node.client().admin().indices().prepareDeleteMapping("_river")
				.setType(name).execute().actionGet();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.debug("Running Cluster Health");
		ClusterHealthResponse clusterHealth = node.client().admin().cluster()
				.health(clusterHealthRequest().waitForGreenStatus())
				.actionGet();
		logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
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
