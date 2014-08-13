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
import static org.elasticsearch.client.Requests.getRequest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.Assert;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

@Test
public class RiverMongoCollectionWithDot extends RiverMongoDBTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoCollectionWithDot(ExecutableType type) {
        super(type);
    }

    private void createDatabase(String database, String collection) {
        logger.debug("createDatabase {}", database);
        try {
            mongoDB = getMongo().getDB(database);
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(collection, null);
            Assert.assertNotNull(mongoCollection);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    private void createRiver(String database, String collection) throws Exception {
        super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON, getRiver(), 3, database, collection, collection);
    }

    private void cleanUp() {
        super.deleteRiver();
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @Test
    public void initialImport() throws Throwable {
        logger.debug("Start initialImport");
        long timestamp = System.currentTimeMillis();
        String database = "db-" + timestamp;
        try {
            String collection = "collection." + timestamp;
            createDatabase(database, collection);

            DBObject dbObject  = new BasicDBObject("name", "richard-" + timestamp);
            mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();

            createRiver(database, collection);
            Thread.sleep(wait);
            refreshIndex(collection);

            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(collection));
            assertThat(response.actionGet().isExists(), equalTo(true));
            refreshIndex(collection);
            logger.info("Request: [{}] - count: [{}]", getRequest(collection).id(id), getNode().client().count(countRequest(collection)).get().getCount());
            GetResponse getResponse = getNode().client().get(getRequest(collection).id(id)).get();
            assertThat(getResponse.isExists(), equalTo(true));

            mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

            Thread.sleep(wait);
            refreshIndex(collection);
            getResponse = getNode().client().get(getRequest(collection).id(id)).get();
            assertThat(getResponse.isExists(), equalTo(false));

        } catch (Throwable t) {
            logger.error("initialImport failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            cleanUp();
        }
    }

    @Test
    public void collectionWithDot_Issue206() throws Throwable {
        logger.debug("Start collectionWithDot_Issue206");
        long timestamp = System.currentTimeMillis();
        String database = "db-" + timestamp;
        try {
            String collection = "collection." + timestamp;
            createDatabase(database, collection);
            createRiver(database, collection);
            DBObject dbObject  = new BasicDBObject("name", "richard-" + timestamp);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            logger.info("dbObject: {}", dbObject.toString());
            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(collection));
            assertThat(response.actionGet().isExists(), equalTo(true));
            refreshIndex(collection);
            logger.info("Request: [{}] - count: [{}]", getRequest(collection).id(id), getNode().client().count(countRequest(collection)).get().getCount());
            GetResponse getResponse = getNode().client().get(getRequest(collection).id(id)).get();
            assertThat(getResponse.isExists(), equalTo(true));

            mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

            Thread.sleep(wait);
            refreshIndex(collection);
            getResponse = getNode().client().get(getRequest(collection).id(id)).get();
            assertThat(getResponse.isExists(), equalTo(false));

        } catch (Throwable t) {
            logger.error("collectionWithDot_Issue206 failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            cleanUp();
        }
    }
}
