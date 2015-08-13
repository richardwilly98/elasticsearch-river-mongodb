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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.count.CountResponse;
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
import com.mongodb.util.JSON;

public class RiverMongoInitialTimestampTest extends RiverMongoDBTestAbstract {

    private static final String TEST_SIMPLE_MONGODB_RIVER_INITIAL_TIMESTAMP_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-initial-timestamp.json";
    private static final String GROOVY_SCRIPT_TYPE = "groovy";
    private static final String JAVASCRIPT_SCRIPT_TYPE = "js";
    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoInitialTimestampTest(ExecutableType type) {
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

    // String script =
    // "def now = new Date(); println 'Now: ${now}'; ctx.document.modified = now.clearTime();";
    @Test(enabled = false) // appears to be broken--fails in isolation
    public void testInitialTimestampInGroovy() throws Throwable {
        logger.debug("Start testInitialTimestampInGroovy");
        String river = "testinitialtimestampgroovyriver-" + System.currentTimeMillis();
        String index = "testinitialtimestampgroovyindex-" + System.currentTimeMillis();
        try {
            String script = "import groovy.time.TimeCategory; use(TimeCategory){def date = new Date() + 5.second; date.time;}";
            super.createRiver(TEST_SIMPLE_MONGODB_RIVER_INITIAL_TIMESTAMP_JSON, river, 3, GROOVY_SCRIPT_TYPE, script, getDatabase(),
                    getCollection(), index, getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            mongoCollection.insert(dbObject);
            Thread.sleep(wait);

            assertThat(getNode().client().admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists(), equalTo(true));

            refreshIndex(index);

            CountResponse countResponse = getNode().client().count(countRequest(index)).actionGet();
            assertThat(countResponse.getCount(), equalTo(0L));

            mongoCollection.remove(dbObject);

            // Wait 5 seconds and store a new document
            Thread.sleep(5000);

            dbObject = (DBObject) JSON.parse(mongoDocument);
            mongoCollection.insert(dbObject);
            Thread.sleep(wait);

            assertThat(getNode().client().admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists(), equalTo(true));
            assertThat(getNode().client().admin().indices().prepareTypesExists(index).setTypes(getDatabase()).execute().actionGet()
                    .isExists(), equalTo(true));

            refreshIndex(index);

            countResponse = getNode().client().count(countRequest(index)).actionGet();
            assertThat(countResponse.getCount(), equalTo(1L));

            mongoCollection.remove(dbObject);
        } catch (Throwable t) {
            logger.error("testInitialTimestampInGroovy failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver(river);
            super.deleteIndex(index);
        }
    }

    // Convert JavaScript types to Java types:
    // http://stackoverflow.com/questions/6730062/passing-common-types-between-java-and-rhino-javascript
    @Test(enabled = false) // breaks following test in suite
    public void testInitialTimestampInJavascript() throws Throwable {
        logger.debug("Start testInitialTimestampInJavascript");
        String river = "testinitialtimestampjavascriptriver-" + System.currentTimeMillis();
        String index = "testinitialtimestampjavascriptindex-" + System.currentTimeMillis();
        try {
            String script = "var date = new Date(); date.setSeconds(date.getSeconds() + 5); new java.lang.Long(date.getTime());";
            super.createRiver(TEST_SIMPLE_MONGODB_RIVER_INITIAL_TIMESTAMP_JSON, river, 3, JAVASCRIPT_SCRIPT_TYPE, script, getDatabase(),
                    getCollection(), index, getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            mongoCollection.insert(dbObject);
            Thread.sleep(wait);

            assertThat(getNode().client().admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists(), equalTo(true));

            refreshIndex(index);

            CountResponse countResponse = getNode().client().count(countRequest(index)).actionGet();
            assertThat(countResponse.getCount(), equalTo(0L));

            mongoCollection.remove(dbObject);

            // Wait 5 seconds and store a new document
            Thread.sleep(5000);

            dbObject = (DBObject) JSON.parse(mongoDocument);
            mongoCollection.insert(dbObject);
            Thread.sleep(wait);

            assertThat(getNode().client().admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists(), equalTo(true));
            assertThat(getNode().client().admin().indices().prepareTypesExists(index).setTypes(getDatabase()).execute().actionGet()
                    .isExists(), equalTo(true));

            refreshIndex(index);

            countResponse = getNode().client().count(countRequest(index)).actionGet();
            assertThat(countResponse.getCount(), equalTo(1L));

            mongoCollection.remove(dbObject);
        } catch (Throwable t) {
            logger.error("testInitialTimestampInJavascript failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver(river);
            super.deleteIndex(index);
        }
    }
}
