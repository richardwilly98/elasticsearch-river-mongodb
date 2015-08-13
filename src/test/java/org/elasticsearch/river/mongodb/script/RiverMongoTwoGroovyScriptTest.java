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
public class RiverMongoTwoGroovyScriptTest extends RiverMongoDBTestAbstract {

    private static final String GROOVY_SCRIPT_TYPE = "groovy";
    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoTwoGroovyScriptTest(ExecutableType type) {
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
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @Test
    public void testTwoRiversGroovyScript() throws Throwable {
        logger.debug("Start testTwoRiversGroovyScript");
        String river1 = "testignorescriptgroovyriver-" + System.currentTimeMillis();
        String index1 = "testignorescriptgroovyindex-" + System.currentTimeMillis();
        String collection1 = "testcollection-" + System.currentTimeMillis();
        Thread.sleep(100);
        String river2 = "testignorescriptgroovyriver-" + System.currentTimeMillis();
        String index2 = "testignorescriptgroovyindex-" + System.currentTimeMillis();
        String collection2 = "testcollection-" + System.currentTimeMillis();
        try {
            logger.debug("Create river {}", river1);
            String script = "ctx.ignore = true";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, river1, 3, getDatabase(), collection1, GROOVY_SCRIPT_TYPE,
                    script, index1, getDatabase());

            logger.debug("Create river {}", river2);
            script = "if (ctx.document.to_be_deleted) { ctx.operation = 'd' }";
            super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, river2, 3, getDatabase(), collection2, GROOVY_SCRIPT_TYPE,
                    script, index2, getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoDB.getCollection(collection1).insert(dbObject);
            Thread.sleep(wait);
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(index1);

            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices().exists(new IndicesExistsRequest(index1));
            assertThat(response.actionGet().isExists(), equalTo(true));
            CountResponse countResponse = getNode().client().count(countRequest(index1)).actionGet();
            logger.info("Document count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(0l));
            mongoDB.getCollection(collection1).remove(dbObject);

            mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            dbObject = (DBObject) JSON.parse(mongoDocument);
            result = mongoDB.getCollection(collection2).insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(index2);

            response = getNode().client().admin().indices().exists(new IndicesExistsRequest(index2));
            assertThat(response.actionGet().isExists(), equalTo(true));

            SearchResponse sr = getNode().client().prepareSearch(index2).setQuery(QueryBuilders.queryString(id).defaultField("_id")).execute().actionGet();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            dbObject.put("to_be_deleted", Boolean.TRUE);
            mongoDB.getCollection(collection2).save(dbObject);

            Thread.sleep(wait);
            refreshIndex(index2);

            countResponse = getNode().client().count(countRequest(index2)).actionGet();
            logger.info("Document count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(0l));

            mongoDB.getCollection(collection2).remove(dbObject);
        } catch (Throwable t) {
            logger.error("testTwoRiversGroovyScript failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver(river1);
            super.deleteIndex(index1);
            logger.debug("End testTwoRiversGroovyScript");
        }
    }
}
