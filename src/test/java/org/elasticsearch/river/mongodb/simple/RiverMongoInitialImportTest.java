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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.testng.Assert;
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

    private DB mongoDB;
    private DBCollection mongoCollection;

    protected RiverMongoInitialImportTest() {
        super("testmongodb-" + System.currentTimeMillis(), "testriver-" + System.currentTimeMillis(), "person-"
                + System.currentTimeMillis(), "personindex-" + System.currentTimeMillis());
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
                GetResponse statusResponse = getNode().client().prepareGet("_river", river, MongoDBRiver.STATUS_ID).execute().actionGet();
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
            Assert.assertEquals(Status.RUNNING, MongoDBRiverHelper.getRiverStatus(getNode().client(), river));
            assertThat(getNode().client().count(countRequest(getIndex())).actionGet().getCount(), equalTo(1l));

            // Check that it syncs the oplog
            DBObject dbObject2 = new BasicDBObject(ImmutableMap.of("name", "Ben"));
            WriteResult result2 = mongoCollection.insert(dbObject2);
            logger.info("WriteResult: {}", result2.toString());
            Thread.sleep(wait);

            refreshIndex();
            Assert.assertEquals(Status.RUNNING, MongoDBRiverHelper.getRiverStatus(getNode().client(), river));
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

    private void createRiver() throws Exception {
        super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);
    }

    private void cleanUp() {
        super.deleteRiver();
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

}
