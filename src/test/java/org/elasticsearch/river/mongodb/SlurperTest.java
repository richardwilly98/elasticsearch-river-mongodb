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

import java.util.List;

import org.bson.types.BSONTimestamp;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

@Test
public class SlurperTest extends RiverMongoDBTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;

    protected SlurperTest() {
        super("testmongodb-" + System.currentTimeMillis(), "testriver-" + System.currentTimeMillis(), "person-"
                + System.currentTimeMillis(), "personindex-" + System.currentTimeMillis());
    }

    private void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(getCollection(), null);
            Assert.assertNotNull(mongoCollection);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    private void createIndex() throws Exception {
        getNode().client().prepareIndex("_river", river, "_meta").setSource(getSettingsString()).execute().actionGet();
    }

    private void cleanUp() {
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @Test
    public void testInitialImport() throws Throwable {
        logger.debug("Start InitialImport");
        try {
            createDatabase();
            createIndex();

            DBObject dbObject1 = new BasicDBObject(ImmutableMap.of("name", "Richard"));
            WriteResult result1 = mongoCollection.insert(dbObject1);
            logger.info("WriteResult: {}", result1.toString());
            Thread.sleep(wait);

            TestSlurper slurper = createSlurper();
            Assert.assertTrue(slurper.assignCollections());
            Assert.assertFalse(slurper.riverHasIndexedSomething());

            new Thread(slurper).start();
            Thread.sleep(wait);

            Assert.assertTrue(slurper.doInitialImportHasBeenCalled());
        } finally {
            cleanUp();
        }
    }

    private String getSettingsString() throws Exception {
        return getJsonSettings(TEST_MONGODB_RIVER_SIMPLE_JSON, String.valueOf(getMongoPort1()), String.valueOf(getMongoPort2()),
                String.valueOf(getMongoPort3()), database, collection, index);
    }

    private TestSlurper createSlurper() throws Exception {
        super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);

        RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                getSettingsString().getBytes(), false).v2());
        RiverName riverName = new RiverName("mongodb", river);
        MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(), index, riverSettings, null);

        return new TestSlurper(mongo.getServerAddressList(), definition, new SharedContext(null, Status.RUNNING), getNode().client());
    }

    private static class TestSlurper extends Slurper {

        public TestSlurper(List<ServerAddress> mongoServers, MongoDBRiverDefinition definition, SharedContext context, Client client) {
            super(mongoServers, definition, context, client);
        }

        private boolean doInitialImportCalled = false;

        public BSONTimestamp doInitialImport() {
            doInitialImportCalled = true;
            return null;
        }

        public boolean doInitialImportHasBeenCalled() {
            return doInitialImportCalled;
        }

    }

}
