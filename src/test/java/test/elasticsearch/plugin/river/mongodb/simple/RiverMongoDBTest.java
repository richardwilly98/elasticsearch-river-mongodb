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

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import test.elasticsearch.plugin.river.mongodb.RiverMongoDBTestAsbtract;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

@Test
public class RiverMongoDBTest extends RiverMongoDBTestAsbtract {

	private final ESLogger logger = Loggers.getLogger(getClass());

	private static final String DATABASE_NAME = "testriver";
	private static final String COLLECTION_NAME = "person";
	private static final String RIVER_NAME = "testmongodb";
	private static final String INDEX_NAME = "personindex";

	private DB mongoDB;
	private DBCollection mongoCollection;

	protected RiverMongoDBTest() {
		super(RIVER_NAME, DATABASE_NAME, COLLECTION_NAME, INDEX_NAME);
	}

	@BeforeClass
	public void createDatabase() {
		logger.debug("createDatabase {}", DATABASE_NAME);
		try {
			mongoDB = getMongo().getDB(DATABASE_NAME);
			mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
			super.createRiver("/test/elasticsearch/plugin/river/mongodb/simple/test-simple-mongodb-river.json");
			logger.info("Start createCollection");
			mongoCollection = mongoDB.createCollection(COLLECTION_NAME, null);
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

	@Test(enabled = false)
	public void mongoCRUDTest() {
		logger.info("Start mongoCRUDTest");
		DBObject dbObject = new BasicDBObject("count", "-1");
		mongoCollection.insert(dbObject, WriteConcern.REPLICAS_SAFE);
		logger.debug("New object inserted: {}", dbObject.toString());
		DBObject dbObject2 = mongoCollection.findOne(new BasicDBObject("_id",
				dbObject.get("_id")));
		Assert.assertEquals(dbObject.get("count"), dbObject2.get("count"));
		mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);
	}

	@Test
	public void simpleBSONObject() throws Throwable {
		logger.debug("Start simpleBSONObject");
		try {
			String mongoDocument = copyToStringFromClasspath("/test/elasticsearch/plugin/river/mongodb/simple/test-simple-mongodb-document.json");
			DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
			WriteResult result = mongoCollection.insert(dbObject);
			Thread.sleep(1000);
			String id = dbObject.get("_id").toString();
			logger.info("WriteResult: {}", result.toString());
			ActionFuture<IndicesExistsResponse> response = getNode().client()
					.admin().indices()
					.exists(new IndicesExistsRequest(INDEX_NAME));
			assertThat(response.actionGet().isExists(), equalTo(true));
			refreshIndex();
			CountResponse countResponse = getNode()
					.client()
					.count(countRequest(INDEX_NAME).query(
							fieldQuery("name", "Richard"))).actionGet();
			logger.info("Document count: {}", countResponse.count());
			countResponse = getNode()
					.client()
					.count(countRequest(INDEX_NAME)
							.query(fieldQuery("_id", id))).actionGet();
			assertThat(countResponse.count(), equalTo(1l));

			mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

			Thread.sleep(1000);
			refreshIndex();
			countResponse = getNode()
					.client()
					.count(countRequest(INDEX_NAME)
							.query(fieldQuery("_id", id))).actionGet();
			logger.debug("Count after delete request: {}", countResponse.count());
			 assertThat(countResponse.count(), equalTo(0L));
		} catch (Throwable t) {
			logger.error("simpleBSONObject failed.", t);
			t.printStackTrace();
			throw t;
		}
	}

}
