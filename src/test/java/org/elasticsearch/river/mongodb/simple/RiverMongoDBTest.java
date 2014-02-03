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
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.BaseRiverMongoDBTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

@Test
public class RiverMongoDBTest extends BaseRiverMongoDBTest {

    private DB mongoDB;
    private DBCollection mongoCollection;

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(getCollection(), null);
            Assert.assertNotNull(mongoCollection);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    @AfterClass
    public void cleanUp() {
        super.deleteRiver();
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @Test(enabled = false)
    public void mongoCRUDTest() {
        logger.info("Start mongoCRUDTest");
        DBObject dbObject = new BasicDBObject("count", "-1");
        mongoCollection.insert(dbObject, WriteConcern.REPLICAS_SAFE);
        logger.debug("New object inserted: {}", dbObject.toString());
        DBObject dbObject2 = mongoCollection.findOne(new BasicDBObject("_id", dbObject.get("_id")));
        Assert.assertEquals(dbObject.get("count"), dbObject2.get("count"));
        mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);
    }

    @Test
    public void simpleBSONObject() throws Throwable {
        logger.debug("Start simpleBSONObject");
        try {
            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(getIndex()));
            assertThat(response.actionGet().isExists(), equalTo(true));
            refreshIndex();
            SearchRequest search = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.fieldQuery("name", "Richard"))
                    .request();
            SearchResponse searchResponse = getNode().client().search(search).actionGet();
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
            String chinese = (String) searchResponse.getHits().getAt(0).getSource().get("chinese");
            assertThat(chinese, equalTo("中国菜很好吃。"));
            CountResponse countResponse = getNode().client().count(countRequest(getIndex()).query(fieldQuery("_id", id))).actionGet();
            assertThat(countResponse.getCount(), equalTo(1l));

            mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

            Thread.sleep(wait);
            refreshIndex();
            countResponse = getNode().client().count(countRequest(getIndex()).query(fieldQuery("_id", id))).actionGet();
            logger.debug("Count after delete request: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(0L));

        } catch (Throwable t) {
            logger.error("simpleBSONObject failed.", t);
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void collectionWithDot_Issue206() throws Throwable {
        logger.debug("Start collectionWithDot_Issue206");
        long timestamp = System.currentTimeMillis();
        String river = "river_206-" + timestamp;
        String database = "db-" + timestamp;
        try {
            String collection = "collection." + timestamp;
            DB db = getMongo().getDB(database);
            db.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON, river, String.valueOf(getMongoPort1()), String.valueOf(getMongoPort2()),
                    String.valueOf(getMongoPort3()), database, collection, collection);

            DBCollection dottedCollection = db.createCollection(collection, null);
            Assert.assertNotNull(dottedCollection);
            DBObject dbObject = new BasicDBObject("name", "richard-" + timestamp);
            WriteResult result = dottedCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            logger.info("dbObject: {}", dbObject.toString());
            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(collection));
            assertThat(response.actionGet().isExists(), equalTo(true));
            refreshIndex();
            CountResponse countResponse = getNode().client().count(countRequest(collection).query(fieldQuery("_id", id))).get();
            assertThat(countResponse.getCount(), equalTo(1l));

            dottedCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

            Thread.sleep(wait);
            refreshIndex();
            countResponse = getNode().client().count(countRequest(collection).query(fieldQuery("_id", id))).actionGet();
            logger.debug("Count after delete request: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(0L));

        } catch (Throwable t) {
            logger.error("collectionWithDot_Issue206 failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver(river);
            getMongo().dropDatabase(database);
        }
    }

//    @Test(enabled = false)
//    public void loopRegisterRiver() throws Throwable {
//        int count = 100;
//        for (int i = 0; i < count; i++) {
//            registerRiver();
//        }
//    }
//
//    private void registerRiver() throws Throwable {
//        logger.debug("Start registerRiver");
//        long timestamp = System.currentTimeMillis();
//        String river = "river-" + timestamp;
//        String database = "db-" + timestamp;
//        try {
//            String collection = "collection." + timestamp;
//            DB db = getMongo().getDB(database);
//            db.setWriteConcern(WriteConcern.REPLICAS_SAFE);
//            super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON, river, String.valueOf(getMongoPort1()), String.valueOf(getMongoPort2()),
//                    String.valueOf(getMongoPort3()), database, collection, collection);
//
//            DBCollection dottedCollection = db.createCollection(collection, null);
//            Assert.assertNotNull(dottedCollection);
//            DBObject dbObject = new BasicDBObject("name", "richard-" + timestamp);
//            WriteResult result = dottedCollection.insert(dbObject);
//            Thread.sleep(wait);
//            String id = dbObject.get("_id").toString();
//            logger.info("WriteResult: {}", result.toString());
//            logger.info("dbObject: {}", dbObject.toString());
//            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
//                    .exists(new IndicesExistsRequest(collection));
//            assertThat(response.actionGet().isExists(), equalTo(true));
//            refreshIndex();
//            // getNode().client().prepareCount(collection).setQuery(QueryBuilders.queryString(id).defaultField("_id")).get();
//            CountResponse countResponse = getNode().client().count(countRequest(collection).query(fieldQuery("_id", id))).get();
//            assertThat(countResponse.getCount(), equalTo(1l));
//
//            dottedCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);
//
//            Thread.sleep(wait);
//            refreshIndex();
//            countResponse = getNode().client().count(countRequest(collection).query(fieldQuery("_id", id))).actionGet();
//            logger.debug("Count after delete request: {}", countResponse.getCount());
//            assertThat(countResponse.getCount(), equalTo(0L));
//
//        } catch (Throwable t) {
//            logger.error("registerRiver failed.", t);
//            t.printStackTrace();
//            throw t;
//        } finally {
//            super.deleteRiver(river);
//            getMongo().dropDatabase(database);
//        }
//    }
}
