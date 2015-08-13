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
package org.elasticsearch.river.mongodb.simple;

import static org.elasticsearch.client.Requests.countRequest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.testng.Assert;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

@Test
public class RiverMongoInitialImportTest extends RiverMongoDBTestAbstract {

    public static final String TEST_MONGODB_RIVER_SIMPLE_SKIP_INITIAL_IMPORT_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-skip-initial-import.json";
    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoInitialImportTest(ExecutableType type) {
        super(type);
    }

    @Test
    public void initialImport() throws Throwable {
        logger.debug("Start InitialImport");
        try {
            createDatabase();

            DBObject dbObject1 = new BasicDBObject(ImmutableMap.of("name", "Richard"));
            WriteResult result1 = mongoCollection.insert(dbObject1);
            logger.info("WriteResult: {}", result1.toString());
            Thread.sleep(wait);

            // Make sure we're starting out with the river not setup
            if (getNode().client().admin().indices().prepareExists("_river").get().isExists()) {
                GetResponse statusResponse = getNode().client().prepareGet("_river", getRiver(), MongoDBRiver.STATUS_ID).get();
                logger.debug("Exists? {}", statusResponse.isExists());
                Assert.assertFalse(
                        statusResponse.isExists(),
                        "Expected no river but found one "
                                + XContentMapValues.extractValue(MongoDBRiver.TYPE + "." + MongoDBRiver.STATUS_FIELD,
                                        statusResponse.getSourceAsMap()));
            }

            // Setup the river
            createRiver();
            Thread.sleep(wait);

            // Check that it did an initial import successfully
            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(getIndex()));
            assertThat(response.actionGet().isExists(), equalTo(true));
            assertThat(getNode().client().count(countRequest(getIndex())).actionGet().getCount(), equalTo(1l));
            assertThat(MongoDBRiverHelper.getRiverStatus(getNode().client(), getRiver()), equalTo(Status.RUNNING));

            MongoDBRiverDefinition definition = getMongoDBRiverDefinition(TEST_MONGODB_RIVER_SIMPLE_JSON, getDatabase(), getCollection(),
                    getIndex());
            assertThat(MongoDBRiver.getLastTimestamp(getNode().client(), definition), notNullValue());

            // Check that it syncs the oplog
            DBObject dbObject2 = new BasicDBObject(ImmutableMap.of("name", "Ben"));
            WriteResult result2 = mongoCollection.insert(dbObject2);
            logger.info("WriteResult: {}", result2.toString());
            Thread.sleep(wait);

            refreshIndex();
            assertThat(MongoDBRiverHelper.getRiverStatus(getNode().client(), getRiver()), equalTo(Status.RUNNING));
            assertThat(getNode().client().count(countRequest(getIndex())).actionGet().getCount(), equalTo(2l));

            mongoCollection.remove(dbObject1, WriteConcern.REPLICAS_SAFE);

            Thread.sleep(wait);
            refreshIndex();
            assertThat(getNode().client().count(countRequest(getIndex())).actionGet().getCount(), equalTo(1L));

        } catch (Throwable t) {
            logger.error("InitialImport failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            cleanUp();
        }
    }

    /*
     * Test for issue 167 - test-simple-mongodb-river-skip-initial-import.json
     */
    @Test
    public void initialImportFailWithExistingDataInSameIndexType() throws Throwable {
        logger.debug("Start ImportFailWithExistingDataInSameIndexType");
        try {
            super.deleteIndex();
            Assert.assertTrue(getIndicesAdminClient().prepareCreate(getIndex()).get().isAcknowledged());
            refreshIndex();
            MongoDBRiverDefinition definition = getMongoDBRiverDefinition(TEST_MONGODB_RIVER_SIMPLE_JSON, getDatabase(), getCollection(),
                    getIndex());
            String id = getNode().client().prepareIndex(getIndex(), definition.getTypeName()).setSource("name", "John").get().getId();
            Assert.assertNotNull(id);

            createDatabase();
            DBObject dbObject1 = new BasicDBObject(ImmutableMap.of("name", "Richard"));
            WriteResult result1 = mongoCollection.insert(dbObject1);
            logger.info("WriteResult: {}", result1.toString());
            Thread.sleep(wait);

            // Make sure we're starting out with the river not setup
            if (getNode().client().admin().indices().prepareExists("_river").get().isExists()) {
                GetResponse statusResponse = getNode().client().prepareGet("_river", getRiver(), MongoDBRiver.STATUS_ID).get();
                logger.debug("Exists? {}", statusResponse.isExists());
                Assert.assertFalse(
                        statusResponse.isExists(),
                        "Expected no river but found one "
                                + XContentMapValues.extractValue(MongoDBRiver.TYPE + "." + MongoDBRiver.STATUS_FIELD,
                                        statusResponse.getSourceAsMap()));
            }

            // Setup the river
            createRiver();
            Thread.sleep(wait);

            assertThat(MongoDBRiverHelper.getRiverStatus(getNode().client(), getRiver()), equalTo(Status.INITIAL_IMPORT_FAILED));
        } catch (Throwable t) {
            logger.error("ImportFailWithExistingDataInSameIndexType failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            cleanUp();
        }
    }

    /*
     * Test for issue 167 - test-simple-mongodb-river-skip-initial-import.json
     */
    @Test
    public void skipInitialImportSuccessWithExistingDataInSameIndexType() throws Throwable {
        logger.debug("Start skipImportFailWithExistingDataInSameIndexType");
        try {
            super.deleteIndex();
            Assert.assertTrue(getIndicesAdminClient().prepareCreate(getIndex()).get().isAcknowledged());
            refreshIndex();
            MongoDBRiverDefinition definition = getMongoDBRiverDefinition(TEST_MONGODB_RIVER_SIMPLE_JSON, getDatabase(), getCollection(),
                    getIndex());
            String id = getNode().client().prepareIndex(getIndex(), definition.getTypeName()).setSource("name", "John").get().getId();
            Assert.assertNotNull(id);

            createDatabase();
            DBObject dbObject1 = new BasicDBObject(ImmutableMap.of("name", "Richard"));
            WriteResult result1 = mongoCollection.insert(dbObject1);
            logger.info("WriteResult: {}", result1.toString());
            Thread.sleep(wait);

            // Make sure we're starting out with the river not setup
            if (getNode().client().admin().indices().prepareExists("_river").get().isExists()) {
                GetResponse statusResponse = getNode().client().prepareGet("_river", getRiver(), MongoDBRiver.STATUS_ID).get();
                logger.debug("Exists? {}", statusResponse.isExists());
                Assert.assertFalse(
                        statusResponse.isExists(),
                        "Expected no river but found one "
                                + XContentMapValues.extractValue(MongoDBRiver.TYPE + "." + MongoDBRiver.STATUS_FIELD,
                                        statusResponse.getSourceAsMap()));
            }

            // Setup the river
            super.createRiver(TEST_MONGODB_RIVER_SIMPLE_SKIP_INITIAL_IMPORT_JSON);
            Thread.sleep(wait);

            assertThat(MongoDBRiverHelper.getRiverStatus(getNode().client(), getRiver()), equalTo(Status.RUNNING));
        } catch (Throwable t) {
            logger.error("skipImportFailWithExistingDataInSameIndexType failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            cleanUp();
        }
    }

    /*
     * Test for issue 167
     */
    @Test
    public void initialImportWithExistingIndex() throws Throwable {
        logger.debug("Start InitialImportWithExistingIndex");
        try {
            super.deleteIndex();
            Assert.assertTrue(getIndicesAdminClient().prepareCreate(getIndex()).get().isAcknowledged());
            refreshIndex();
            String id = getNode().client().prepareIndex(getIndex(), "dummy-type-" + System.currentTimeMillis()).setSource("name", "John")
                    .get().getId();
            Assert.assertNotNull(id);

            createDatabase();
            DBObject dbObject1 = new BasicDBObject(ImmutableMap.of("name", "Richard"));
            WriteResult result1 = mongoCollection.insert(dbObject1);
            logger.info("WriteResult: {}", result1.toString());
            Thread.sleep(wait);

            // Make sure we're starting out with the river not setup
            if (getNode().client().admin().indices().prepareExists("_river").get().isExists()) {
                GetResponse statusResponse = getNode().client().prepareGet("_river", getRiver(), MongoDBRiver.STATUS_ID).get();
                logger.debug("Exists? {}", statusResponse.isExists());
                Assert.assertFalse(
                        statusResponse.isExists(),
                        "Expected no river but found one "
                                + XContentMapValues.extractValue(MongoDBRiver.TYPE + "." + MongoDBRiver.STATUS_FIELD,
                                        statusResponse.getSourceAsMap()));
            }

            // Setup the river
            createRiver();
            Thread.sleep(wait);

            assertThat(MongoDBRiverHelper.getRiverStatus(getNode().client(), getRiver()), equalTo(Status.RUNNING));
            assertThat(getNode().client().count(countRequest(getIndex())).actionGet().getCount(), equalTo(2l));

            // Check that it syncs the oplog
            DBObject dbObject2 = new BasicDBObject(ImmutableMap.of("name", "Ben"));
            WriteResult result2 = mongoCollection.insert(dbObject2);
            logger.info("WriteResult: {}", result2.toString());
            Thread.sleep(wait);

            refreshIndex();
            assertThat(MongoDBRiverHelper.getRiverStatus(getNode().client(), getRiver()), equalTo(Status.RUNNING));
            assertThat(getNode().client().count(countRequest(getIndex())).actionGet().getCount(), equalTo(3l));

            mongoCollection.remove(dbObject1, WriteConcern.REPLICAS_SAFE);

            Thread.sleep(wait);
            refreshIndex();
            assertThat(getNode().client().count(countRequest(getIndex())).actionGet().getCount(), equalTo(2L));

        } catch (Throwable t) {
            logger.error("InitialImportWithExistingIndex failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            cleanUp();
        }
    }

    private void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(getCollection(), new BasicDBObject());
            Assert.assertNotNull(mongoCollection);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    private void createRiver() throws Exception {
        super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);
    }

    private void cleanUp() {
        super.deleteRiver();
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

}
