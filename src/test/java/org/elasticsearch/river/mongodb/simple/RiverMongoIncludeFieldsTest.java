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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.bson.types.ObjectId;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
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

@Test
public class RiverMongoIncludeFieldsTest extends RiverMongoDBTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;
    protected boolean dropCollectionOption = true;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoIncludeFieldsTest(ExecutableType type) {
        super(type);
    }

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(TEST_MONGODB_RIVER_INCLUDE_FIELDS_JSON, getRiver(), 3,
                    (Object) "[\"include-field-1\", \"include-field-2\"]", (Object) getDatabase(), (Object) getCollection(),
                    (Object) getIndex(), (Object) getDatabase());
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(getCollection(), new BasicDBObject());
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

    @Test
    public void testIncludeFields() throws Throwable {
        logger.debug("Start testIncludeFields");
        try {
            DBObject dbObject = new BasicDBObject();
            dbObject.put("include-field-1", System.currentTimeMillis());
            dbObject.put("include-field-2", System.currentTimeMillis());
            dbObject.put("field-3", System.currentTimeMillis());
            mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            assertThat(getNode().client().admin().indices().exists(new IndicesExistsRequest(getIndex())).actionGet().isExists(),
                    equalTo(true));
            refreshIndex();

            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id"))
                    .execute().actionGet();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            Map<String, Object> object = sr.getHits().getHits()[0].sourceAsMap();
            assertThat(object.containsKey("include-field-1"), equalTo(true));
            assertThat(object.containsKey("include-field-2"), equalTo(true));
            assertThat(object.containsKey("field-3"), equalTo(false));

            // Update Mongo object
            dbObject = mongoCollection.findOne(new BasicDBObject("_id", new ObjectId(id)));
            dbObject.put("field-4", System.currentTimeMillis());
            mongoCollection.save(dbObject);
            Thread.sleep(wait);

            sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id")).execute()
                    .actionGet();
            logger.debug("SearchResponse {}", sr.toString());
            totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            object = sr.getHits().getHits()[0].sourceAsMap();
            assertThat(object.containsKey("include-field-1"), equalTo(true));
            assertThat(object.containsKey("include-field-2"), equalTo(true));
            assertThat(object.containsKey("field-3"), equalTo(false));
            assertThat(object.containsKey("field-4"), equalTo(false));
        } catch (Throwable t) {
            logger.error("testIncludeFields failed.", t);
            t.printStackTrace();
            throw t;
        }
    }

}
