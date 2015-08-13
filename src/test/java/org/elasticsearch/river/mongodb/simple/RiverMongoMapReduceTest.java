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

import java.util.Random;

import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.WriteConcern;

@Test
public class RiverMongoMapReduceTest extends RiverMongoDBTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;
    private DBCollection mongoCollection2;

    @Factory(dataProvider = "onlyVanillaMongo")
    public RiverMongoMapReduceTest(ExecutableType type) {
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

    @Test(groups = { "slow" })
    public void mapReduceTest() throws Throwable {
        logger.debug("Start mapReduceTest");
        try {
            final long MAX = 20L;
            final String outputCollection = "order_totals";
            Random random = new Random();
            for (long i = 0; i < MAX; i++) {
                DBObject object = BasicDBObjectBuilder.start().add("name", "order-" + i).add("cust_id", random.nextInt(10))
                        .add("amount", random.nextInt(500)).get();
                mongoCollection.insert(object);
                Thread.sleep(200);
            }
            Thread.sleep(wait);
            refreshIndex();
            assertThat(executableType.name() + " inputCollection is indexed",
                    getNode().client().admin().indices().prepareTypesExists(getIndex()).setTypes(mongoCollection.getName()).get()
                            .isExists(), equalTo(true));

            String map = "function() { emit(this.cust_id, this.amount); }";
            String reduce = "function (key, values) { return Array.sum( values ) }";

            MapReduceCommand cmd = new MapReduceCommand(mongoCollection, map, reduce, outputCollection,
                    MapReduceCommand.OutputType.REPLACE, null);

            MapReduceOutput out = mongoCollection.mapReduce(cmd);
            logger.debug("MapReduceOutput: {}", out);
            Thread.sleep(wait);
            refreshIndex();
            assertThat(executableType.name() + " outputCollection is indexed",
                    getNode().client().admin().indices().prepareTypesExists(getIndex()).setTypes(outputCollection).get().isExists(),
                    equalTo(true));

            logger.debug("*** Index/type [{}/{}] count [{}]", getIndex(), mongoCollection.getName(),
                    getNode().client().prepareCount(getIndex()).setTypes(mongoCollection.getName()).get().getCount());

            logger.debug("*** Index/type [{}/{}] count [{}]", getIndex(), outputCollection, getNode().client().prepareCount(getIndex())
                    .setTypes(outputCollection).get().getCount());

            assertThat(executableType.name() + " inputCollection items indexed",
                    getNode().client().prepareCount(getIndex()).setTypes(mongoCollection.getName()).get().getCount(),
                    equalTo(mongoCollection.count()));
            assertThat(executableType.name() + " outputCollection items indexed",
                    getNode().client().prepareCount(getIndex()).setTypes(outputCollection).get().getCount(),
                    equalTo(mongoDB.getCollection(outputCollection).count()));
        } catch (Throwable t) {
            logger.error("mapReduceTest failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
        }
    }

}
