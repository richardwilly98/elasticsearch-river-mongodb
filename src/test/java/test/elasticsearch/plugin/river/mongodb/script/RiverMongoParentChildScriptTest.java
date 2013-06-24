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

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
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
public class RiverMongoParentChildScriptTest extends RiverMongoDBTestAsbtract {

	private static final String QUERY_BOOKS_JSON = "/test/elasticsearch/plugin/river/mongodb/script/query-books.json";
	private static final String BOOK1_DOCUMENT_JSON = "/test/elasticsearch/plugin/river/mongodb/script/test-mongodb-book1-document.json";
	private static final String AUTHOR_DOCUMENT_JSON = "/test/elasticsearch/plugin/river/mongodb/script/test-mongodb-author-document.json";
	private static final String BOOKS_MAPPING_JSON = "/test/elasticsearch/plugin/river/mongodb/script/books-mapping.json";
	private static final String AUTHORS_MAPPING_JSON = "/test/elasticsearch/plugin/river/mongodb/script/authors-mapping.json";
	
	private static final String DATABASE_NAME = "testparentchild";
	private static final String AUTHORS_COLLECTION = "authors";
	private static final String AUTHORS_RIVER_NAME = "authors_river";
	// private static final String AUTHORS_INDEX_NAME = "authors_index";
	private static final String INDEX_NAME = "authors_books_index";
	private static final String AUTHOR_TYPE = "author";
	private static final String BOOKS_COLLECTION = "books";
	private static final String BOOKS_RIVER_NAME = "books_river";
	// private static final String BOOKS_INDEX_NAME = "books_index";
	private static final String BOOK_TYPE = "book";

	private DB mongoDB;
	private DBCollection mongoAuthorsCollection;
	private DBCollection mongoBooksCollection;

	protected RiverMongoParentChildScriptTest() {
		super(AUTHORS_RIVER_NAME, DATABASE_NAME, AUTHORS_COLLECTION, INDEX_NAME);
	}

	@BeforeClass
	public void setupEnvironment() {
		createDatabase();
		createIndicesAndMappings();
	}

	private void createDatabase() {
		logger.debug("createDatabase {}", DATABASE_NAME);
		try {
			mongoDB = getMongo().getDB(DATABASE_NAME);
			mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
			logger.info("Start createCollection");
			mongoAuthorsCollection = mongoDB.createCollection(
					AUTHORS_COLLECTION, null);
			Assert.assertNotNull(mongoAuthorsCollection);
			mongoBooksCollection = mongoDB.createCollection(BOOKS_COLLECTION,
					null);
			Assert.assertNotNull(mongoBooksCollection);
		} catch (Throwable t) {
			logger.error("createDatabase failed.", t);
		}
	}

	private void createIndicesAndMappings() {
		try {
			getNode().client().admin().indices().prepareCreate(INDEX_NAME)
					.execute().actionGet();

			getNode()
					.client()
					.admin()
					.indices()
					.preparePutMapping(INDEX_NAME)
					.setType(AUTHOR_TYPE)
					.setSource(
							getJsonSettings(AUTHORS_MAPPING_JSON))
					.execute().actionGet();

			getNode()
					.client()
					.admin()
					.indices()
					.preparePutMapping(INDEX_NAME)
					.setType(BOOK_TYPE)
					.setSource(
							getJsonSettings(BOOKS_MAPPING_JSON))
					.execute().actionGet();

			super.createRiver(
					TEST_MONGODB_RIVER_SIMPLE_WITH_TYPE_JSON,
					AUTHORS_RIVER_NAME,
					(Object) String.valueOf(getMongoPort1()),
					(Object) String.valueOf(getMongoPort2()),
					(Object) String.valueOf(getMongoPort3()),
					(Object) DATABASE_NAME, (Object) AUTHORS_COLLECTION,
					(Object) INDEX_NAME, (Object) AUTHOR_TYPE);

			String script = "if(ctx.document._parentId) { ctx._parent = ctx.document._parentId; delete ctx.document._parentId;}";
			super.createRiver(
					TEST_MONGODB_RIVER_WITH_SCRIPT_JSON,
					BOOKS_RIVER_NAME, (Object) String.valueOf(getMongoPort1()),
					(Object) String.valueOf(getMongoPort2()),
					(Object) String.valueOf(getMongoPort3()),
					(Object) DATABASE_NAME, (Object) BOOKS_COLLECTION,
					(Object) "js",script, (Object) INDEX_NAME, (Object) BOOK_TYPE);
		} catch (Throwable t) {
			logger.error("createIndicesAndMappings failed.", t);
			Assert.fail("createIndicesAndMappings failed.", t);
		}

	}

	@AfterClass
	public void cleanUp() {
		// super.deleteRiver();
		logger.info("Drop database " + mongoDB.getName());
		mongoDB.dropDatabase();
	}

	@Test
	public void testParentChildScript() throws Throwable {
		logger.debug("Start testParentChildScript");
		try {
			String authorDocument = copyToStringFromClasspath(AUTHOR_DOCUMENT_JSON);
			DBObject dbObject = (DBObject) JSON.parse(authorDocument);
			WriteResult result = mongoAuthorsCollection.insert(dbObject);
			Thread.sleep(wait);
			String authorId = dbObject.get("_id").toString();
			logger.info("WriteResult: {}", result.toString());
			refreshIndex(INDEX_NAME);

			ActionFuture<IndicesExistsResponse> response = getNode().client()
					.admin().indices()
					.exists(new IndicesExistsRequest(INDEX_NAME));
			assertThat(response.actionGet().isExists(), equalTo(true));

			SearchResponse sr = getNode().client().prepareSearch(INDEX_NAME)
					.setQuery(fieldQuery("_id", authorId)).execute()
					.actionGet();
			logger.debug("SearchResponse {}", sr.toString());
			long totalHits = sr.getHits().getTotalHits();
			logger.debug("TotalHits: {}", totalHits);
			assertThat(totalHits, equalTo(1l));

			String book1Document = copyToStringFromClasspath(BOOK1_DOCUMENT_JSON);
			dbObject = (DBObject) JSON.parse(book1Document);
			result = mongoBooksCollection.insert(dbObject);
			Thread.sleep(wait);
			String bookId = dbObject.get("_id").toString();
			logger.info("WriteResult: {}", result.toString());
			refreshIndex(INDEX_NAME);

			response = getNode().client().admin().indices()
					.exists(new IndicesExistsRequest(INDEX_NAME));
			assertThat(response.actionGet().isExists(), equalTo(true));

			sr = getNode().client().prepareSearch(INDEX_NAME)
					.setQuery(fieldQuery("_id", bookId)).execute().actionGet();
			logger.debug("SearchResponse {}", sr.toString());
			totalHits = sr.getHits().getTotalHits();
			logger.debug("TotalHits: {}", totalHits);
			assertThat(totalHits, equalTo(1l));

			sr = getNode()
					.client()
					.prepareSearch(INDEX_NAME)
					.setTypes(AUTHOR_TYPE)
					.setSource(
							getJsonSettings(QUERY_BOOKS_JSON))
					.execute().actionGet();
			logger.debug("SearchResponse {}", sr.toString());
			totalHits = sr.getHits().getTotalHits();
			logger.debug("Filtered - TotalHits: {}", totalHits);

			mongoBooksCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);
			mongoAuthorsCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);
		} catch (Throwable t) {
			logger.error("testParentChildScript failed.", t);
			t.printStackTrace();
			throw t;
		} finally {
			super.deleteRiver(AUTHORS_RIVER_NAME);
			super.deleteRiver(BOOKS_RIVER_NAME);
			super.deleteIndex();
		}
	}

}
