package test.elasticsearch.plugin.river.mongodb;

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.bson.types.ObjectId;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

@Test
public class RiverMongoWithGridFSTest extends RiverMongoDBTestAsbtract {

	private final ESLogger logger = Loggers.getLogger(getClass());

	private static final String DATABASE_NAME = "testgridfs";
	private static final String COLLECTION_NAME = "fs";
	private static final String RIVER_NAME = "testgridfs";
	private static final String INDEX_NAME = "testattachmentindex";

	private DB mongoDB;
	private DBCollection mongoCollection;

	@BeforeClass
	public void createDatabase() {
		logger.debug("createDatabase {}", DATABASE_NAME);
		try {
			mongoDB = getMongo().getDB(DATABASE_NAME);
			logger.debug("Create river {}", RIVER_NAME);
			super.createRiver(RIVER_NAME, "test-gridfs-mongodb-river.json");
			ActionFuture<IndicesExistsResponse> response = getNode().client()
					.admin().indices()
					.exists(new IndicesExistsRequest(INDEX_NAME));
			assertThat(response.actionGet().isExists(), equalTo(true));
			logger.info("Start createCollection");
			mongoCollection = mongoDB.createCollection(COLLECTION_NAME, null);
			Assert.assertNotNull(mongoCollection);
		} catch (Throwable t) {
			logger.error("createDatabase failed.", t);
		}
	}

	@AfterClass
	public void cleanUp() {
		super.deleteRiver(INDEX_NAME);
		logger.info("Drop database " + mongoDB.getName());
		mongoDB.dropDatabase();
	}

	@Test
	public void testImportAttachment() throws Exception {
		logger.debug("*** testImportAttachment ***");
		byte[] content = copyToBytesFromClasspath("/test/elasticsearch/plugin/river/mongodb/test-attachment.html");
		logger.debug("Content in bytes: {}", content.length);
		GridFS gridFS = new GridFS(mongoDB);
		GridFSInputFile in = gridFS.createFile(content);
		in.setFilename("test-attachment.html");
		in.setContentType("text/html");
		in.save();
		in.validate();
		
		String id = in.getId().toString();
		logger.debug("GridFS in: {}", in);
		logger.debug("Document created with id: {}", id);

		GridFSDBFile out = gridFS.findOne(in.getFilename());
		logger.debug("GridFS from findOne: {}", out);
		out = gridFS.findOne(new ObjectId(id));
		logger.debug("GridFS from findOne: {}", out);
		Assert.assertEquals(out.getId(), in.getId());

		Thread.sleep(5000);
		
		getNode().client().admin().indices()
				.refresh(new RefreshRequest(INDEX_NAME));

		CountResponse countResponse = getNode().client()
				.count(countRequest(INDEX_NAME))
				.actionGet();
		logger.debug("Index total count: {}", countResponse.count());
		assertThat(countResponse.count(), equalTo(1l));
		
		countResponse = getNode().client()
				.count(countRequest(INDEX_NAME).query(fieldQuery("_id", id)))
				.actionGet();
		logger.debug("Index count for id {}: {}", id, countResponse.count());
		assertThat(countResponse.count(), equalTo(1l));
		
		SearchResponse response = getNode().client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.queryString("Aliquam")).execute().actionGet();
		logger.debug("SearchResponse {}", response.toString());
		logger.debug("TotalHits: {}", response.hits().getTotalHits());
		assertThat(response.hits().getTotalHits(), equalTo(1l));

		gridFS.remove(new ObjectId(id));

		Thread.sleep(1000);
		getNode().client().admin().indices()
				.refresh(new RefreshRequest(INDEX_NAME));
		countResponse = getNode()
				.client()
				.count(countRequest(INDEX_NAME)
						.query(fieldQuery("_id", id))).actionGet();
		logger.debug("Count after delete request: {}", countResponse.count());
		 assertThat(countResponse.count(), equalTo(0L));
	}

}
