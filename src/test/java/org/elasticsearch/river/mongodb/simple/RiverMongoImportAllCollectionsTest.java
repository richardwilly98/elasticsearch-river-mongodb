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

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
public class RiverMongoImportAllCollectionsTest extends RiverMongoDBTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;
    private DBCollection mongoCollection2;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoImportAllCollectionsTest(ExecutableType type) {
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
            mongoCollection2 = mongoDB.createCollection("collection-" + System.currentTimeMillis(), new BasicDBObject());
            Assert.assertNotNull(mongoCollection2);
            createRiver(TEST_MONGODB_RIVER_IMPORT_ALL_COLLECTION_JSON, getRiver(), 3, getDatabase(), getIndex());
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    @AfterClass
    public void cleanUp() {
        logger.info("Drop database " + mongoDB.getName());
        super.deleteRiver();
        mongoDB.dropDatabase();
    }

    @Test
    public void importAllCollectionsTest() throws Throwable {
        logger.debug("Start importAllCollectionsTest");
        try {
            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            WriteResult result = mongoCollection.insert(dbObject);
            Thread.sleep(wait);
            String id = dbObject.get("_id").toString();
            logger.info("WriteResult: {}", result.toString());
            refreshIndex();
            Assert.assertNotNull(getNode().client().prepareGet(getIndex(), mongoCollection.getName(), id).get().getId());

            DBObject dbObject2 = (DBObject) JSON.parse(mongoDocument);
            WriteResult result2 = mongoCollection2.insert(dbObject2);
            Thread.sleep(wait);
            String id2 = dbObject2.get("_id").toString();
            logger.info("WriteResult: {}", result2.toString());
            refreshIndex();
            Assert.assertNotNull(getNode().client().prepareGet(getIndex(), mongoCollection2.getName(), id2).get().getId());

            mongoCollection.remove(dbObject);
            Thread.sleep(wait);
            refreshIndex();
            assertThat(getNode().client().prepareCount(getIndex()).setTypes(mongoCollection.getName()).setQuery(QueryBuilders.queryString(id).defaultField("_id"))
                    .get().getCount(), equalTo(0L));

            mongoCollection2.remove(dbObject2);
            Thread.sleep(wait);
            refreshIndex();
            assertThat(getNode().client().prepareCount(getIndex()).setTypes(mongoCollection2.getName()).setQuery(QueryBuilders.queryString(id2).defaultField("_id"))
                    .get().getCount(), equalTo(0L));
        } catch (Throwable t) {
            logger.error("importAllCollectionsTest failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
        }
    }

}
