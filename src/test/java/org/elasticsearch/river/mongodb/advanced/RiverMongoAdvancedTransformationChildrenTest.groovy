package org.elasticsearch.river.mongodb.advanced

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath
import org.elasticsearch.index.query.QueryBuilders
import static org.elasticsearch.search.sort.SortOrder.ASC

import org.bson.types.ObjectId
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract
import org.elasticsearch.search.SearchHit
import org.testng.Assert
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test

import com.gmongo.GMongo
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.WriteConcern

class RiverMongoAdvancedTransformationChildrenTest extends RiverMongoDBTestAbstract {
	// This Groovy script is available in src/test/scripts. 
	// It will be copied by Maven plugin build-helper-maven-plugin in target/config/scripts
	static final String GROOVY_SCRIPT = "advanced-transformation-groovy-script"
	static final int WAIT = 1000

	private def db
	private DBCollection dbCollection

	protected RiverMongoAdvancedTransformationChildrenTest() {
	}

	@BeforeClass
	public void createDatabase() {
        logger.debug("Create database {} and collection {}", database, collection)
		db = new GMongo(mongo).getDB(database)
		db.setWriteConcern(WriteConcern.REPLICAS_SAFE)
		dbCollection = db.createCollection(collection, [:])
		Assert.assertNotNull(dbCollection)
	}


	@AfterClass
	public void cleanUp() {
		db.dropDatabase()
	}

	@Test
	public void "Test insert-update-delete with children attribute"() {
		def river = "testchildrentransformationscriptgroovyriver-" + System.currentTimeMillis()
		def index = "testchildrentransformationscriptgroovyindex-" + System.currentTimeMillis()
		try {

			logger.debug("Create river {}", river)

			// Create river
			node.client().admin().indices().prepareCreate(index).execute().actionGet()

			node.client()
					.admin()
					.indices()
					.preparePutMapping(index)
					.setType("tweet")
					.setSource(
					getJsonSettings("/org/elasticsearch/river/mongodb/advanced/tweets-mapping.json"))
					.execute().actionGet()

			createRiver(
					RiverMongoAdvancedTransformationGroovyScriptTest.TEST_MONGODB_RIVER_WITH_ADVANCED_TRANSFORMATION_JSON, river,
					mongoPort1.toString(), mongoPort2.toString(), mongoPort3.toString(),
					"[\"author\"]",
					database, collection, RiverMongoAdvancedTransformationGroovyScriptTest.GROOVY_SCRIPT_TYPE, GROOVY_SCRIPT, index, database
					)

			// -- INSERT --
			def document = [
				name: "Pablo",
				tweets: [
					[_id: "51c8ddbae4b0548e8d233181", text: "foo"],
					[_id: "51c8ddbae4b0548e8d233182", text: "bar"],
					[_id: "51c8ddbae4b0548e8d233183", text: "zoo"],
				]
			]

			// Insert test document in mongodb
			def dbObject = new BasicDBObject(document)
			def result = dbCollection.insert(dbObject)
			logger.info("WriteResult: $result")
			Thread.sleep(WAIT)
			// Assert index exists
			def request = new IndicesExistsRequest(index)
			assert node.client().admin().indices().exists(request).actionGet().isExists() == true
			// Search data by parent
			refreshIndex(index)
			def parentId = dbObject.get("_id").toString()
			def response = node.client().prepareSearch(index).setQuery(QueryBuilders.queryString(parentId).defaultField("_parent")).addSort("text", ASC).execute().actionGet()
			logger.debug("SearchResponse $response")
			// Asserts data
			assert response.hits.totalHits == 3
			SearchHit[] hits = response.hits.hits
			assert "bar" == hits[0].sourceAsMap().text
			assert "foo" == hits[1].sourceAsMap().text
			assert "zoo" == hits[2].sourceAsMap().text

			// -- UPDATES SCENARIOS --
			// #1: Replace whole document
			document.tweets[0].text = "fool"
			dbCollection.update([_id: new ObjectId(parentId)], document)
			Thread.sleep(WAIT)
			refreshIndex(index)
			response = node.client().prepareSearch(index).setQuery(QueryBuilders.queryString(parentId).defaultField("_parent")).addSort("text", ASC).execute().actionGet()
			logger.debug("SearchResponse $response")
			// Asserts data
			assert response.hits.totalHits == 3
			hits = response.hits.hits
			assert "bar"  == hits[0].sourceAsMap().text
			assert "fool" == hits[1].sourceAsMap().text
			assert "zoo"  == hits[2].sourceAsMap().text
			// #2: Push one value to the array
			dbCollection.update([_id: new ObjectId(parentId)], [$push: [tweets:[_id: "51c8ddbae4b0548e8d233184", text: "abc"]]])
			Thread.sleep(WAIT)
			refreshIndex(index)
			response = node.client().prepareSearch(index).setQuery(QueryBuilders.queryString(parentId).defaultField("_parent")).addSort("text", ASC).execute().actionGet()
			logger.debug("SearchResponse $response")
			// Asserts data
			assert response.hits.totalHits == 4
			hits = response.hits.hits
			assert "abc"  == hits[0].sourceAsMap().text
			assert "bar"  == hits[1].sourceAsMap().text
			assert "fool" == hits[2].sourceAsMap().text
			assert "zoo"  == hits[3].sourceAsMap().text

			// #3: Pull one value from the array
			dbCollection.update([_id: new ObjectId(parentId)], [$pull: [tweets:[text: "bar"]]])
			Thread.sleep(WAIT)
			refreshIndex(index)
			response = node.client().prepareSearch(index).setQuery(QueryBuilders.queryString(parentId).defaultField("_parent")).addSort("text", ASC).execute().actionGet()
			logger.debug("SearchResponse $response")
			// Asserts data
			assert response.hits.totalHits == 3
			hits = response.hits.hits
			assert "abc"  == hits[0].sourceAsMap().text
			assert "fool" == hits[1].sourceAsMap().text
			assert "zoo"  == hits[2].sourceAsMap().text

			// -- DELETE --
			dbCollection.remove([_id: new ObjectId(parentId)])
			Thread.sleep(WAIT)
			refreshIndex(index)
            assert !node.client().prepareGet(index, "author", parentId).get().exists
			response = node.client().prepareSearch(index).setQuery(QueryBuilders.queryString(parentId).defaultField("_parent")).execute().actionGet()
			logger.debug("SearchResponse $response")
			// Asserts data
			assert response.hits.totalHits == 0

		} catch (Throwable t) {
			Assert.fail("Test insert-update-delete with children attribute failed", t)
		} finally {
			super.deleteRiver(river)
			super.deleteIndex(index)
		}
	}
}
