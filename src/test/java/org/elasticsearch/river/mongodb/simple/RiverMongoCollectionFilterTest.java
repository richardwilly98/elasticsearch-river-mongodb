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
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
public class RiverMongoCollectionFilterTest extends RiverMongoDBTestAbstract {

    private static final String TEST_SIMPLE_MONGODB_RIVER_COLLECTION_FILTER_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-collection-filter.json";
    private DB mongoDB;
    private DBCollection mongoCollection;
    private Object collectionFilterWithPrefix = "{'o.lang':'de'}";
    private Object collectionFilterNoPrefix = "{'lang':'de'}";

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoCollectionFilterTest(ExecutableType type) {
        super(type);
    }

    @BeforeMethod
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

    @AfterMethod
    private void cleanUp() {
        logger.trace("Delete river {}", getRiver());
        try {
            deleteRiver();
            logger.trace("Drop database " + mongoDB.getName());
            mongoDB.dropDatabase();
        } catch (Throwable t) {
            logger.warn("cleanUp failed.", t);
        }
    }

    @Test
    public void collectionFilterWithPrefixTest() throws Throwable {
        collectionFilterTest(collectionFilterWithPrefix);
    }

    @Test
    public void collectionFilterNoPrefixTest() throws Throwable {
        collectionFilterTest(collectionFilterNoPrefix);
    }

    private void collectionFilterTest(Object filter) throws Throwable {
        logger.debug("Start CollectionFilter");
        try {
            DBObject dbObject1 = new BasicDBObject(ImmutableMap.of("name", "Bernd", "lang", "de"));
            WriteResult result1 = mongoCollection.insert(dbObject1);
            logger.info("WriteResult: {}", result1.toString());
            dbObject1 = new BasicDBObject(ImmutableMap.of("name", "Richard", "lang", "fr"));
            result1 = mongoCollection.insert(dbObject1);
            logger.info("WriteResult: {}", result1.toString());
            Thread.sleep(wait);

            createRiver(filter);
            Thread.sleep(wait);

            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(getIndex()));
            assertThat(response.actionGet().isExists(), equalTo(true));
            refreshIndex();
            assertThat(getNode().client().count(countRequest(getIndex())).actionGet().getCount(), equalTo(1l));
        } catch (Throwable t) {
            logger.error("CollectionFilter failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            cleanUp();
        }
    }

    private void createRiver(Object filter) throws Exception {
        super.createRiver(TEST_SIMPLE_MONGODB_RIVER_COLLECTION_FILTER_JSON, getRiver(), 3, (Object) getDatabase(),
                (Object) getCollection(), filter, (Object) getIndex(), (Object) getDatabase());
    }

}
