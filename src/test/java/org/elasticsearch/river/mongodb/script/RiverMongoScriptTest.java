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
package org.elasticsearch.river.mongodb.script;

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

@Test
public class RiverMongoScriptTest extends RiverMongoDBTestAbstract {

    private static final String JAVASCRIPT_SCRIPT_TYPE = "js";
    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoScriptTest(ExecutableType type) {
        super(type);
    }

    @BeforeClass
    public void createDatabase() {
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

    @AfterClass
    public void cleanUp() {
        // super.deleteRiver(river);
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @Test
    public void testIgnoreScript() throws Throwable {
        logger.debug("Start testIgnoreScript");
        try {
            logger.debug("Create river {}", getRiver());
            String script = "ctx.ignore = true;";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, getRiver(), 3, getDatabase(), getCollection(),
                    JAVASCRIPT_SCRIPT_TYPE, script, getIndex(), getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(getIndex());

            assertThat(getNode().client().admin().indices().prepareExists(getIndex()).get().isExists(), equalTo(true));
            CountResponse countResponse = getNode().client().count(countRequest(getIndex())).actionGet();
            logger.info("Document count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(0l));

            mongoCollection.remove(dbObject);

        } catch (Throwable t) {
            logger.error("testIgnoreScript failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver();
            super.deleteIndex();
        }
    }

    @Test
    public void testUpdateAttribute() throws Throwable {
        logger.debug("Start testUpdateAttribute");
        try {
            logger.debug("Create river {}", getRiver());
            String script = "ctx.document.score = 200;";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, getRiver(), 3, getDatabase(), getCollection(),
                    JAVASCRIPT_SCRIPT_TYPE, script, getIndex(), getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(getIndex());

            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(getIndex()));
            assertThat(response.actionGet().isExists(), equalTo(true));

            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id"))
                    .execute().actionGet();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            assertThat(sr.getHits().getHits()[0].sourceAsMap().containsKey("score"), equalTo(true));
            int score = Integer.parseInt(sr.getHits().getHits()[0].sourceAsMap().get("score").toString());

            logger.debug("Score: {}", score);
            assertThat(score, equalTo(200));

            mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

        } catch (Throwable t) {
            logger.error("testUpdateAttribute failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver();
            super.deleteIndex();
        }
    }

    @Test
    public void testRemoveAttribute() throws Throwable {
        logger.debug("Start testRemoveAttribute");
        try {
            logger.debug("Create river {}", getRiver());
            String script = "delete ctx.document.score;";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, getRiver(), 3, getDatabase(), getCollection(),
                    JAVASCRIPT_SCRIPT_TYPE, script, getIndex(), getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(getIndex());

            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(getIndex()));
            assertThat(response.actionGet().isExists(), equalTo(true));

            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id"))
                    .execute().actionGet();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            assertThat(sr.getHits().getHits()[0].sourceAsMap().containsKey("score"), equalTo(false));
            mongoCollection.remove(dbObject);
        } catch (Throwable t) {
            logger.error("testRemoveAttribute failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver();
            super.deleteIndex();
        }
    }

    @Test
    public void testRenameAttribute() throws Throwable {
        logger.debug("Start testRenameAttribute");
        String river = "testrenameattributescriptriver-" + System.currentTimeMillis();
        String index = "testrenameattributescriptindex-" + System.currentTimeMillis();
        try {
            logger.debug("Create river {}", river);
            String script = "ctx.document.score2 = ctx.document.score; delete ctx.document.score;";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, river, 3, getDatabase(), getCollection(), JAVASCRIPT_SCRIPT_TYPE, script, index, getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(index);

            assertThat(getNode().client().admin().indices().prepareExists(index).get().isExists(), equalTo(true));

            SearchResponse sr = getNode().client().prepareSearch(index).setQuery(QueryBuilders.queryString(id).defaultField("_id"))
                    .execute().actionGet();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            assertThat(sr.getHits().getHits()[0].sourceAsMap().containsKey("score2"), equalTo(true));
            mongoCollection.remove(dbObject);
        } catch (Throwable t) {
            logger.error("testRenameAttribute failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver(river);
            super.deleteIndex(index);
        }
    }

    @Test
    public void testDeleteDocument() throws Throwable {
        logger.debug("Start testDeleteDocument");
        try {
            logger.debug("Create river {}", getRiver());
            String script = "if (ctx.document.to_be_deleted == true) { ctx.operation = 'd' };";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, getRiver(), 3, getDatabase(), getCollection(),
                    JAVASCRIPT_SCRIPT_TYPE, script, getIndex(), getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(getIndex());

            assertThat(getNode().client().admin().indices().prepareExists(getIndex()).get().isExists(), equalTo(true));

            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id"))
                    .execute().actionGet();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            dbObject.put("to_be_deleted", Boolean.TRUE);
            mongoCollection.save(dbObject);

            Thread.sleep(wait);
            refreshIndex(getIndex());

            CountResponse countResponse = getNode().client().count(countRequest(getIndex())).actionGet();
            logger.info("Document count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(0l));

            mongoCollection.remove(dbObject);
        } catch (Throwable t) {
            logger.error("testDeleteDocument failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver();
            super.deleteIndex();
        }
    }

}
