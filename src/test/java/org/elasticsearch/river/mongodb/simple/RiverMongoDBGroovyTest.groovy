package org.elasticsearch.river.mongodb.simple

import com.gmongo.GMongo
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.WriteConcern
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.testng.annotations.*
import org.testng.Assert
import org.elasticsearch.search.SearchHit

import org.elasticsearch.index.query.QueryBuilders

class RiverMongoDBGroovyTest extends RiverMongoDBTestAbstract {

	static final int WAIT = 1000

	private def db
	private DBCollection dbCollection

	@BeforeClass
	public void createDatabase() {
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
	public void "simple mongodb river test"() {
		try {
			// Create river
			createRiver(
					"/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river.json", river,
					mongoPort1.toString(), mongoPort2.toString(), mongoPort3.toString(),
					database, collection, index
					)

			def document = [
				name: 'test-groovy',
				score: 99
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
			refreshIndex()
			def id = dbObject.get("_id").toString()
			def response = node.client().prepareSearch(index).setQuery(QueryBuilders.queryString(id).defaultField("_id")).execute().actionGet()
			logger.debug("SearchResponse $response")

			// Asserts data
			assert response.hits.totalHits == 1
			SearchHit[] hits = response.hits.hits
			assert "test-groovy" == hits[0].sourceAsMap().name

		} finally {
			super.deleteRiver()
			super.deleteIndex()
		}
	}
}
