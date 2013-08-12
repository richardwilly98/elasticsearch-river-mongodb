package test.elasticsearch.plugin.river.mongodb.simple

import com.gmongo.GMongo
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.WriteConcern
import test.elasticsearch.plugin.river.mongodb.RiverMongoDBTestAsbtract
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.testng.annotations.*
import org.testng.Assert
import org.elasticsearch.search.SearchHit

import static org.elasticsearch.index.query.QueryBuilders.fieldQuery

//@Test
class RiverMongoDBGroovyTest extends RiverMongoDBTestAsbtract {

	static final int WAIT = 1000

	private def db
	private DBCollection dbCollection

	protected RiverMongoDBGroovyTest() {
		super("testriver-"     + System.currentTimeMillis(),
		"testdatabase-"  + System.currentTimeMillis(),
		"documents-"    + System.currentTimeMillis(),
		"testindex-"    + System.currentTimeMillis())
	}

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
	public void "my test"() {
		try {
			// Create river
			createRiver(
					"/test/elasticsearch/plugin/river/mongodb/simple/test-simple-mongodb-river.json", river,
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
			def response = node.client().prepareSearch(index).setQuery(fieldQuery("_id", id)).execute().actionGet()
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
