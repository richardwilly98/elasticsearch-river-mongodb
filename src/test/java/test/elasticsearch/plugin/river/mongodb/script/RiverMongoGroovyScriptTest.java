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
package test.elasticsearch.plugin.river.mongodb.script;

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import test.elasticsearch.plugin.river.mongodb.RiverMongoDBTestAsbtract;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

@Test
public class RiverMongoGroovyScriptTest extends RiverMongoDBTestAsbtract {

	private static final String GROOVY_SCRIPT_TYPE = "groovy";
	private DB mongoDB;
	private DBCollection mongoCollection;

	protected RiverMongoGroovyScriptTest() {
		super("testgroovyriver-" + System.currentTimeMillis(),
				"testgroovydatabase-" + System.currentTimeMillis(),
				"groovydocuments-" + System.currentTimeMillis(),
				"testgroovyindex-" + System.currentTimeMillis());
	}

	@BeforeClass
	public void createDatabase() {
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

	@AfterClass
	public void cleanUp() {
		logger.info("Drop database " + mongoDB.getName());
		mongoDB.dropDatabase();
	}

	@Test
	public void testIgnoreScript() throws Throwable {
		logger.debug("Start testIgnoreScript");
		String river = "testignorescriptgroovyriver-" + System.currentTimeMillis();
		String index = "testignorescriptgroovyindex-" + System.currentTimeMillis();
		try {
			logger.debug("Create river {}", river);
			String script = "ctx.ignore = true";
			super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, river,
					String.valueOf(getMongoPort1()),
					String.valueOf(getMongoPort2()),
					String.valueOf(getMongoPort3()), getDatabase(),
					getCollection(), GROOVY_SCRIPT_TYPE, script, index,
					getDatabase());

			String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
			DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
			WriteResult result = mongoCollection.insert(dbObject);
			Thread.sleep(wait);
			logger.info("WriteResult: {}", result.toString());
			refreshIndex(index);

			ActionFuture<IndicesExistsResponse> response = getNode().client()
					.admin().indices()
					.exists(new IndicesExistsRequest(index));
			assertThat(response.actionGet().isExists(), equalTo(true));
			CountResponse countResponse = getNode().client()
					.count(countRequest(index)).actionGet();
			logger.info("Document count: {}", countResponse.getCount());
			assertThat(countResponse.getCount(), equalTo(0l));

			mongoCollection.remove(dbObject);

		} catch (Throwable t) {
			logger.error("testIgnoreScript failed.", t);
			t.printStackTrace();
			throw t;
		} finally {
			super.deleteRiver(river);
			super.deleteIndex(index);
			logger.debug("End testIgnoreScript");
		}
	}

	@Test
	public void testUpdateAttribute() throws Throwable {
		logger.debug("Start testUpdateAttribute");
		String river = "testupdateattributegroovyriver-" + System.currentTimeMillis();
		String index = "testupdateattributegroovyindex-" + System.currentTimeMillis();
		try {
			logger.debug("Create river {}", river);
			String script = "def now = new Date(); println 'Now: ${now}'; ctx.document.modified = now.clearTime();";
			super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, river,
					String.valueOf(getMongoPort1()),
					String.valueOf(getMongoPort2()),
					String.valueOf(getMongoPort3()), getDatabase(),
					getCollection(), GROOVY_SCRIPT_TYPE, script, index,
					getDatabase());

			String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
			DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
			WriteResult result = mongoCollection.insert(dbObject);
			Thread.sleep(wait);
			String id = dbObject.get("_id").toString();
			logger.info("WriteResult: {}", result.toString());
			refreshIndex(index);

			ActionFuture<IndicesExistsResponse> response = getNode().client()
					.admin().indices()
					.exists(new IndicesExistsRequest(index));
			assertThat(response.actionGet().isExists(), equalTo(true));

			SearchResponse sr = getNode().client().prepareSearch(index)
					.setQuery(fieldQuery("_id", id)).execute().actionGet();
			logger.debug("SearchResponse {}", sr.toString());
			long totalHits = sr.getHits().getTotalHits();
			logger.debug("TotalHits: {}", totalHits);
			assertThat(totalHits, equalTo(1l));

			assertThat(
					sr.getHits().getHits()[0].sourceAsMap().containsKey(
							"modified"), equalTo(true));
			String modified = sr.getHits().getHits()[0].sourceAsMap()
					.get("modified").toString();

			logger.debug("modified: {}", modified);

			mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

		} catch (Throwable t) {
			logger.error("testUpdateAttribute failed.", t);
			t.printStackTrace();
			throw t;
		} finally {
			super.deleteRiver(river);
			super.deleteIndex(index);
			logger.debug("End testUpdateAttribute");
		}
	}

	@Test
	public void testDeleteDocument() throws Throwable {
		logger.debug("Start testDeleteDocument");
		String river = "testdeletedocumentgroovyriver-" + System.currentTimeMillis();
		String index = "testdeletedocumentgroovyindex-" + System.currentTimeMillis();
		try {
			logger.debug("Create river {}", river);
			String script = "if (ctx.document.to_be_deleted) { ctx.operation = 'd' }";
			super.createRiver(TEST_MONGODB_RIVER_WITH_SCRIPT_JSON, river,
					String.valueOf(getMongoPort1()),
					String.valueOf(getMongoPort2()),
					String.valueOf(getMongoPort3()), getDatabase(),
					getCollection(), GROOVY_SCRIPT_TYPE, script, index,
					getDatabase());

			String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
			DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
			WriteResult result = mongoCollection.insert(dbObject);
			Thread.sleep(wait);
			String id = dbObject.get("_id").toString();
			logger.info("WriteResult: {}", result.toString());
			refreshIndex(index);

			ActionFuture<IndicesExistsResponse> response = getNode().client()
					.admin().indices()
					.exists(new IndicesExistsRequest(index));
			assertThat(response.actionGet().isExists(), equalTo(true));

			SearchResponse sr = getNode().client().prepareSearch(index)
					.setQuery(fieldQuery("_id", id)).execute().actionGet();
			logger.debug("SearchResponse {}", sr.toString());
			long totalHits = sr.getHits().getTotalHits();
			logger.debug("TotalHits: {}", totalHits);
			assertThat(totalHits, equalTo(1l));

			dbObject.put("to_be_deleted", Boolean.TRUE);
			mongoCollection.save(dbObject);

			Thread.sleep(wait);
			refreshIndex(index);

			CountResponse countResponse = getNode().client()
					.count(countRequest(index)).actionGet();
			logger.info("Document count: {}", countResponse.getCount());
			assertThat(countResponse.getCount(), equalTo(0l));

			mongoCollection.remove(dbObject);
		} catch (Throwable t) {
			logger.error("testDeleteDocument failed.", t);
			t.printStackTrace();
			throw t;
		} finally {
			super.deleteRiver(river);
			super.deleteIndex(index);
			logger.debug("End testDeleteDocument");
		}
	}

}
