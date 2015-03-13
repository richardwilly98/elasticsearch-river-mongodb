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
public class RiverMongoExcludeFieldsTest extends RiverMongoDBTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;
    protected boolean dropCollectionOption = true;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoExcludeFieldsTest(ExecutableType type) {
        super(type);
    }

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(TEST_MONGODB_RIVER_EXCLUDE_FIELDS_JSON, getRiver(), 3,
                    (Object) "[\"exclude-field-1\", \"exclude-field-2\"]", (Object) getDatabase(), (Object) getCollection(),
                    (Object) getIndex(), (Object) getDatabase());
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

    @Test
    public void testExcludeFields() throws Throwable {
        logger.debug("Start testExcludeFields");
        try {
            DBObject dbObject = new BasicDBObject();
            dbObject.put("exclude-field-1", System.currentTimeMillis());
            dbObject.put("exclude-field-2", System.currentTimeMillis());
            dbObject.put("include-field-1", System.currentTimeMillis());
            mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            assertThat(getNode().client().admin().indices().exists(new IndicesExistsRequest(getIndex())).actionGet().isExists(),
                    equalTo(true));
            refreshIndex();

            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id"))
                    .get();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            Map<String, Object> object = sr.getHits().getHits()[0].sourceAsMap();
            assertThat(object.containsKey("exclude-field-1"), equalTo(false));
            assertThat(object.containsKey("exclude-field-2"), equalTo(false));
            assertThat(object.containsKey("include-field-1"), equalTo(true));

            // Update Mongo object
            dbObject = mongoCollection.findOne(new BasicDBObject("_id", new ObjectId(id)));
            dbObject.put("include-field-2", System.currentTimeMillis());
            mongoCollection.save(dbObject);
            Thread.sleep(wait);

            sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id")).get();
            logger.debug("SearchResponse {}", sr.toString());
            totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            object = sr.getHits().getHits()[0].sourceAsMap();
            assertThat(object.containsKey("exclude-field-1"), equalTo(false));
            assertThat(object.containsKey("exclude-field-2"), equalTo(false));
            assertThat(object.containsKey("include-field-1"), equalTo(true));
            assertThat(object.containsKey("include-field-2"), equalTo(true));
        } catch (Throwable t) {
            logger.error("testExcludeFields failed.", t);
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testUpdateOnExcludedFieldIsIgnored() throws Throwable {
        logger.debug("Start testUpdateOnExcludedFieldIsIgnored");
        try {
            DBObject dbObject = new BasicDBObject();
            dbObject.put("exclude-field-1", System.currentTimeMillis());
            dbObject.put("exclude-field-2", System.currentTimeMillis());
            dbObject.put("include-field-1", System.currentTimeMillis());
            mongoCollection.insert(dbObject);
            mongoCollection.update(new BasicDBObject("_id", dbObject.get("_id")),
                    new BasicDBObject("$set", new BasicDBObject("exclude-field-1", System.currentTimeMillis())));

            Thread.sleep(wait);

            String id = dbObject.get("_id").toString();
            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id"))
                    .setVersion(true).get();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);

            assertThat(totalHits, equalTo(1l));
            assertThat(sr.getHits().getHits()[0].getVersion(), equalTo(1l));
        } catch (Throwable t) {
            logger.error("testUpdateOnExcludedFieldIsIgnored failed.", t);
            t.printStackTrace();
            throw t;
        }
    }

}
