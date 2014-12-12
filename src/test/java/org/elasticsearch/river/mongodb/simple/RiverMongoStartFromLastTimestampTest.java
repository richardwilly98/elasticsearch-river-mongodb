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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.lang.reflect.Field;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiversService;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.river.mongodb.Timestamp;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.testng.Assert;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class RiverMongoStartFromLastTimestampTest extends RiverMongoDBTestAbstract {
    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoStartFromLastTimestampTest(ExecutableType type) {
        super(type);
    }

    private void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(getCollection(), null);
            Assert.assertNotNull(mongoCollection);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    private void cleanUp() {
        super.deleteRiver();
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    private Map<String, Object> getSource(Client client, String index, String typeName, String id) {
        return client.prepareGet(index, typeName, id).get().getSource();
    }

    @Test
    public void testSlurperResumesFromLastTimestamp() throws Throwable {
        Client client = getNode().client();

        logger.debug("Start testSlurperResumesFromLastTimestamp");
        try {
            // Create the river
            super.deleteIndex();
            MongoDBRiverDefinition definition = getMongoDBRiverDefinition(TEST_MONGODB_RIVER_SIMPLE_JSON, getDatabase(), getCollection(),
                    getIndex());
            String id = client.prepareIndex(getIndex(), definition.getTypeName()).setSource("name", "John").get().getId();
            Assert.assertNotNull(id);

            createDatabase();
            super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);
            Thread.sleep(wait);
            assertThat(MongoDBRiverHelper.getRiverStatus(client, getRiver()), equalTo(Status.RUNNING));

            // Add an object to the database
            DBObject dbObject1 = new BasicDBObject(ImmutableMap.of("name", "Foo"));
            WriteResult result1 = mongoCollection.insert(dbObject1);
            logger.info("WriteResult: {}", result1.toString());
            Thread.sleep(wait);

            // - Should have the document itself
            Map<String, Object> esObject1 = getSource(client, getIndex(), definition.getTypeName(), dbObject1.get("_id").toString());
            assertThat(esObject1, is(notNullValue()));
            assertThat(esObject1.get("name"), equalTo(dbObject1.get("name")));
            // - Should have a timestamp in the status
            Timestamp firstTimestamp = MongoDBRiver.getLastTimestamp(client, definition);
            assertThat(firstTimestamp, is(notNullValue(Timestamp.class)));

            // Stop the river now
            MongoDBRiverHelper.setRiverStatus(client, getRiver(), Status.STOPPED);
            Thread.sleep(wait);

            // Add another object to the database
            DBObject dbObject2 = new BasicDBObject(ImmutableMap.of("name", "Bar"));
            WriteResult result2 = mongoCollection.insert(dbObject2);
            logger.info("WriteResult: {}", result2.toString());
            Thread.sleep(wait);

            // Start the river
            MongoDBRiverHelper.setRiverStatus(client, getRiver(), Status.RUNNING);
            // Force start until #407 is merged
            restartRiver(getRiver());
            Thread.sleep(wait);

            // Check that the object was loaded
            // - Should have the second document now
            Map<String, Object> esObject2 = getSource(client, getIndex(), definition.getTypeName(), dbObject2.get("_id").toString());
            assertThat(esObject2, is(notNullValue()));
            assertThat(esObject2.get("name"), equalTo(dbObject2.get("name")));
            // - Should have a newer timestamp in the status
            Timestamp lastTimestamp = MongoDBRiver.getLastTimestamp(client, definition);
            assertThat(lastTimestamp, is(greaterThan(firstTimestamp)));
        } catch (Throwable t) {
            logger.error("testSlurperResumesFromLastTimestamp failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            cleanUp();
        }
    }

    private void restartRiver(String river) {
        try {
            RiversService riversService = ((InternalNode) getNode()).injector().getInstance(RiversService.class);
            Field riversInjectorsField = RiversService.class.getDeclaredField("riversInjectors");
            riversInjectorsField.setAccessible(true);
            Map<String, Injector> riversInjectors = (Map<String, Injector>) riversInjectorsField.get(riversService);
            Injector riverInjector = riversInjectors.get(new RiverName(MongoDBRiver.TYPE, river));
            MongoDBRiver riverInstance = riverInjector.getInstance(MongoDBRiver.class);
            riverInstance.start();
        } catch (Throwable t) {
            logger.error("Cannot force a restart of the river", t);
            throw new IllegalStateException(t);
        }
    }
}
