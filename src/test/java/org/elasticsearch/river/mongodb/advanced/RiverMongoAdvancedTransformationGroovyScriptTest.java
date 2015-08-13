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
package org.elasticsearch.river.mongodb.advanced;

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
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
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

@Test
public class RiverMongoAdvancedTransformationGroovyScriptTest extends RiverMongoDBTestAbstract {

    private static final String GROOVY_SCRIPT_TYPE = "groovy";
    public static final String TEST_MONGODB_RIVER_WITH_ADVANCED_TRANSFORMATION_JSON = "/org/elasticsearch/river/mongodb/advanced/test-mongodb-river-with-advanced-transformation.json";
    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoAdvancedTransformationGroovyScriptTest(ExecutableType type) {
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
    public void testSimpleTransformationScript() throws Throwable {
        logger.debug("Start testSimpleTransformationScript");
        String river = "testsimpletransformationscriptgroovyriver-" + System.currentTimeMillis();
        String index = "testsimpletransformationscriptgroovyindex-" + System.currentTimeMillis();
        try {
            logger.debug("Create river {}", river);
            String script = "ctx.documents << [data: [id: 12345, name: '99'], operation: 'i'] ";
            script += "<< [data: [id: 6666, name: 'document-ignored'], ignore: true] ";
            super.createRiver(TEST_MONGODB_RIVER_WITH_ADVANCED_TRANSFORMATION_JSON, river, 3, (Object) "[]", getDatabase(), getCollection(),
                    GROOVY_SCRIPT_TYPE, script, index, getDatabase());

            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            logger.info("WriteResult: {}", result.toString());
            refreshIndex(index);

            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices().exists(new IndicesExistsRequest(index));
            assertThat(response.actionGet().isExists(), equalTo(true));
            CountResponse countResponse = getNode().client().count(countRequest(index)).actionGet();
            logger.info("Document count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(2l));

            mongoCollection.remove(dbObject);

        } catch (Throwable t) {
            logger.error("testSimpleTransformationScript failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            super.deleteRiver(river);
            super.deleteIndex(index);
            logger.debug("End testSimpleTransformationScript");
        }
    }

}
