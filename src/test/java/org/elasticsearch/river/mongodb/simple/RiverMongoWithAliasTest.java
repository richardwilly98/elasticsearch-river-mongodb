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

import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
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
public class RiverMongoWithAliasTest extends RiverMongoDBTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;
    private final String realIndex;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoWithAliasTest(ExecutableType type) {
        super(type);
        realIndex = "real-" + getIndex();
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

    private void createAlias() throws Throwable {
        getNode().client().admin().indices().prepareCreate(realIndex).execute().get();
        getNode().client().admin().indices().prepareAliases().addAlias(realIndex, getIndex()).execute().get();
    }

    private void deleteAlias() throws Throwable {
        getNode().client().admin().indices().prepareDelete(getIndex()).execute().actionGet();
    }

    @Test
    public void simpleRiverWithAlias() throws Throwable {
        logger.debug("Start simpleRiverWithAlias");
        try {
            createAlias();
            super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);
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
            SearchRequest search = getNode().client().prepareSearch(getIndex()).setQuery(new QueryStringQueryBuilder("Richard").defaultField("name"))
                    .request();
            SearchResponse searchResponse = getNode().client().search(search).actionGet();
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

            mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);
            Thread.sleep(wait);

            refreshIndex();
            GetResponse getResponse = getNode().client().get(getRequest(getIndex()).id(id)).get();
            logger.debug("Get request for id {}: {}", id, getResponse.isExists());
            assertThat(getResponse.isExists(), equalTo(false));
//            CountResponse countResponse = getNode().client().count(countRequest(getIndex()).query(fieldQuery("_id", id))).actionGet();
//            logger.debug("Count after delete request: {}", countResponse.getCount());
//            assertThat(countResponse.getCount(), equalTo(0L));

        } catch (Throwable t) {
            logger.error("simpleRiverWithAlias failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            deleteAlias();
            super.deleteRiver();
        }
    }
}
