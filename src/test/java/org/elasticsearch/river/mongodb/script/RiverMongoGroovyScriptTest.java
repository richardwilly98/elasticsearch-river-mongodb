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

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

//@Test
public class RiverMongoGroovyScriptTest extends RiverMongoDBTestAbstract {

    private static final String GROOVY_SCRIPT_TYPE = "groovy";
    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoGroovyScriptTest(ExecutableType type) {
        super(type);
    }

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    @AfterClass
    public void cleanUp() {
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @BeforeMethod
    public void createCollection() {
        mongoCollection = mongoDB.createCollection(getCollection(), new BasicDBObject());
        Assert.assertNotNull(mongoCollection);
    }

    @AfterMethod
    public void dropCollection() {
        logger.trace("*** AfterMethod - clean-up");
        super.deleteRiver();
        super.deleteIndex();
        mongoCollection.drop();
    }

    @Test
    public void testIgnoreScript() throws Throwable {
        logger.debug("Start testIgnoreScript");
        try {
            logger.debug("Create river {}", getRiver());
            String script = "ctx.ignore = true";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, getRiver(), 3, getDatabase(), getCollection(), GROOVY_SCRIPT_TYPE,
                    script, getIndex(), getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            dbObject.put("test", "testIgnoreScript");
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(getIndex());

            IndicesExistsResponse response = getNode().client().admin().indices().prepareExists(getIndex()).get();
            assertThat(response.isExists(), equalTo(true));
            CountResponse countResponse = getNode().client().count(countRequest(getIndex())).get();
            logger.info("Document count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(0l));
        } catch (Throwable t) {
            logger.error("testIgnoreScript failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            logger.debug("End testIgnoreScript");
        }
    }

    @Test
    public void testUpdateAttribute() throws Throwable {
        logger.debug("Start testUpdateAttribute");
        try {
            logger.debug("Create river {}", getRiver());
            String script = "def now = new Date(); println 'Now: ${now}'; ctx.document.modified = now.clearTime();";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, getRiver(), 3, getDatabase(), getCollection(), GROOVY_SCRIPT_TYPE,
                    script, getIndex(), getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            dbObject.put("test", "testUpdateAttribute");
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(getIndex());

            IndicesExistsResponse response = getNode().client().admin().indices().prepareExists(getIndex()).get();
            assertThat(response.isExists(), equalTo(true));

            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id")).get();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            assertThat(sr.getHits().getHits()[0].sourceAsMap().containsKey("modified"), equalTo(true));
            String modified = sr.getHits().getHits()[0].sourceAsMap().get("modified").toString();

            logger.debug("modified: {}", modified);

        } catch (Throwable t) {
            logger.error("testUpdateAttribute failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            logger.debug("End testUpdateAttribute");
        }
    }

    @Test
    public void testDeleteDocument() throws Throwable {
        logger.debug("Start testDeleteDocument");
        try {
            logger.debug("Create river {}", getIndex());
            String script = "if (ctx.document.to_be_deleted) { ctx.operation = 'd' }";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, getRiver(), 3, getDatabase(), getCollection(), GROOVY_SCRIPT_TYPE,
                    script, getIndex(), getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            dbObject.put("test", "testDeleteDocument");
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(getIndex());

            IndicesExistsResponse response = getNode().client().admin().indices().prepareExists(getIndex()).get();
            assertThat(response.isExists(), equalTo(true));

            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id")).get();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            dbObject.put("to_be_deleted", Boolean.TRUE);
            mongoCollection.save(dbObject);

            Thread.sleep(wait);
            refreshIndex(getIndex());

            CountResponse countResponse = getNode().client().count(countRequest(getIndex())).get();
            logger.info("Document count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(0l));
        } catch (Throwable t) {
            logger.error("testDeleteDocument failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            logger.debug("End testDeleteDocument");
        }
    }

}
