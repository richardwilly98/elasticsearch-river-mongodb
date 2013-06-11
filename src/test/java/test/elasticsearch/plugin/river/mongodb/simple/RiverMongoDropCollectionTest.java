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
package test.elasticsearch.plugin.river.mongodb.simple;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import test.elasticsearch.plugin.river.mongodb.RiverMongoDBTestAsbtract;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

@Test
public class RiverMongoDropCollectionTest extends RiverMongoDBTestAsbtract {

	private final ESLogger logger = Loggers.getLogger(getClass());
	private final static long wait = 4000;

	private DB mongoDB;
	private DBCollection mongoCollection;
	protected boolean dropCollectionOption = true;

	protected RiverMongoDropCollectionTest() {
		super("drop-river-"+ System.currentTimeMillis(), "drop-river-"+ System.currentTimeMillis(), "drop-collection"+ System.currentTimeMillis(), "drop-index-"+ System.currentTimeMillis());
	}

	protected RiverMongoDropCollectionTest(String river, String database,
			String collection, String index) {
		super(river, database, collection, index);
	}

	@BeforeClass
	public void createDatabase() {
		logger.debug("createDatabase {}", getDatabase());
		try {
			mongoDB = getMongo().getDB(getDatabase());
			mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
			super.createRiver(
					"/test/elasticsearch/plugin/river/mongodb/simple/test-simple-mongodb-river-drop-collection.json",
					getRiver(),
					(Object) String.valueOf(getMongoPort1()),
					(Object) String.valueOf(getMongoPort2()),
					(Object) String.valueOf(getMongoPort3()),
					(Object) dropCollectionOption,
					(Object) getDatabase(), (Object) getCollection(),
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
	public void testDropCollection() throws Throwable {
		logger.debug("Start testDropCollection");
		try {
			String mongoDocument = copyToStringFromClasspath("/test/elasticsearch/plugin/river/mongodb/simple/test-simple-mongodb-document.json");
			DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
			mongoCollection.insert(dbObject);
			Thread.sleep(wait);

			assertThat(getNode().client()
					.admin().indices()
					.exists(new IndicesExistsRequest(getIndex())).actionGet().isExists(), equalTo(true));
			assertThat(getNode().client().admin().indices().prepareTypesExists(getIndex()).setTypes(getDatabase()).execute().actionGet().isExists(), equalTo(true));
			mongoCollection.drop();
			Thread.sleep(wait);
			refreshIndex();
			assertThat(getNode().client().admin().indices().prepareTypesExists(getIndex()).setTypes(getDatabase()).execute().actionGet().isExists(), equalTo(!dropCollectionOption));
		} catch (Throwable t) {
			logger.error("testDropCollection failed.", t);
			t.printStackTrace();
			throw t;
		}
	}

}
