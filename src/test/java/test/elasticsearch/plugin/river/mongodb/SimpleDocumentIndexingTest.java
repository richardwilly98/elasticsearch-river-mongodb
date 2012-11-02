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

package test.elasticsearch.plugin.river.mongodb;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.node.Node;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

@Test
public class SimpleDocumentIndexingTest {

	private final ESLogger logger = Loggers.getLogger(getClass());

	private Node node;
	private Mongo mongo;
	private DB testDB;

	@BeforeClass
	public void setupServer() {
		System.out.println("Start ES server setup");
		logger.info("Start ES server setup");
		node = nodeBuilder()
				.local(true)
				.settings(
						settingsBuilder()
								.put("path.data", "target/data")
								.put("cluster.name",
										"test-cluster-"
												+ NetworkUtils
														.getLocalAddress())
								.put("gateway.type", "none")).node();
	}

	@BeforeClass
	public void setupMongoClient() throws Exception {
		System.out.println("Start MongoDB client");
		logger.info("Start mongo client");
		String mongoAdminUser = "admin";
		String mongoAdminPassword = "Skipper2000";
		List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
		mongoServers.add(new ServerAddress(MongoDBRiver.DEFAULT_DB_HOST,
				MongoDBRiver.DEFAULT_DB_PORT));
		mongo = new Mongo(mongoServers);
		DB adminDb = mongo.getDB(MongoDBRiver.MONGODB_ADMIN);
		CommandResult cmd = adminDb.authenticateCommand(mongoAdminUser,
				mongoAdminPassword.toCharArray());
		if (!cmd.ok()) {
			logger.warn("Autenticatication failed for {}: {}",
					MongoDBRiver.MONGODB_ADMIN, cmd.getErrorMessage());
		}
		testDB = mongo.getDB("testriver");
	}

	@AfterClass
	public void closeServer() {
		node.close();
	}

	// private Client client;
	private String INDEX_NAME = "testindex";
	private String INDEX_TYPE = "testattachment";

	//
	// private File filename = new File("resources/test-document.pdf");
	// private String contentType = "application/pdf";

	// @BeforeClass
	// public void beforeClass() {
	// logger.info("Initiate client.");
	// // String cluster;
	// Settings s = ImmutableSettings.settingsBuilder()
	// // .put("cluster.name", cluster)
	// .build();
	// client = new TransportClient(s);
	// ((TransportClient) client)
	// .addTransportAddress(new InetSocketTransportAddress(
	// "localhost", 9300));
	//
	// try {
	// logger.info("Creating index [{}]", INDEX_NAME);
	// client.admin().indices().create(new CreateIndexRequest(INDEX_NAME))
	// .actionGet();
	//
	// } catch (Exception ex) {
	// logger.warn("already exists", ex);
	// }
	//
	// try {
	// client.admin().indices().preparePutMapping(INDEX_NAME)
	// .setType(INDEX_TYPE).setSource(getMapping()).execute()
	// .actionGet();
	// } catch (ElasticSearchException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } catch (Throwable e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	//
	// @AfterClass
	// public void afterClass() {
	// // logger.info("Deleting index [{}]", INDEX_NAME);
	// // client.admin().indices().delete(new DeleteIndexRequest(INDEX_NAME))
	// // .actionGet();
	// }

	@BeforeMethod
	public void createRiverAndDropCollection() throws Exception {
		System.out.println("Start createRiverAndDropCollection");
		createRiver();
		dropCollection();
	}

	private void createRiver() throws Exception {
		System.out.println("Start createRiver");
		String mapping = copyToStringFromClasspath("/test/elasticsearch/plugin/river/mongodb/test-simple-mongodb-river.json");
		node.client().prepareIndex("_river", "testmongodb", "_meta")
				.setSource(mapping).execute().actionGet();
		System.out.println("Running Cluster Health");
        ClusterHealthResponse clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        System.out.println("Done Cluster Health, status " + clusterHealth.status());
	}

//	@BeforeMethod
//	@AfterMethod
	private void dropCollection() {
		System.out.println("Start dropCollection");
		logger.info("Drop collection {}", "person");
		DBCollection collection = testDB.getCollection("person");
		if (collection != null) {
			collection.drop();
		}
	}

	@AfterMethod
	public void deleteRiverAndIndex() {
		System.out.println("Start deleteRiverAndIndex");
	}

	@Test
	public void importAttachment() throws Throwable {
		System.out.println("Start importAttachment");
		try {
			// DB test = mongo.getDB("testriver");
			DBCollection collection = testDB.getCollection("person");
			String mongoDocument = copyToStringFromClasspath("/test/elasticsearch/plugin/river/mongodb/test-simple-mongodb-document.json");
			WriteResult result = collection.insert((DBObject) JSON
					.parse(mongoDocument));
			logger.info("WriteResult: {}", result);
			System.out.println("WriteResult: " + result);
			node.client().admin().indices()
			.refresh(new RefreshRequest("personindex"));
			ActionFuture<IndicesExistsResponse> response = node.client()
					.admin().indices()
					.exists(new IndicesExistsRequest("personindex"));
//			assertThat(response.actionGet().isExists(), equalTo(true));
			CountResponse countResponse = node
					.client()
					.count(countRequest("personindex").query(
							fieldQuery("name", "Richard"))).actionGet();
			System.out.println("Document count: " + countResponse.count());
			logger.info("Document count: {}", countResponse.count());
//			assertThat(countResponse.count(), equalTo(1l));

			Thread.sleep(2 * 1000);
		} catch (Throwable t) {
			logger.error("importAttachment failed.", t);
			t.printStackTrace();
			throw t;
		}
		/*
		 * String id = "1"; try { byte[] content = getBytesFromFile(filename);
		 * String encodedContent = Base64.encodeBytes(content); XContentBuilder
		 * b = jsonBuilder().startObject(); b.startObject("content");
		 * b.field("content_type", contentType); b.field("title",
		 * filename.getName()); b.field("author", "pippo"); b.field("content",
		 * encodedContent); b.endObject(); b.field("filename",
		 * filename.getName()); b.field("contentType", contentType);
		 * b.startObject("metadata"); b.field("firstName", "pippo");
		 * b.field("lastName", "PLUTO"); b.endObject(); b.endObject();
		 * IndexRequestBuilder irb = node.client().prepareIndex(INDEX_NAME,
		 * INDEX_TYPE).setSource(b); // IndexRequestBuilder irb =
		 * client.prepareIndex(INDEX_NAME, // INDEX_TYPE, id).setSource(b);
		 * IndexResponse indexResponse = irb.execute().actionGet();
		 * logger.info("Index response: {} - {} - {}", indexResponse.getIndex(),
		 * indexResponse.getType(), indexResponse.getId()); id =
		 * indexResponse.getId(); } catch (IOException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 * node.client().admin().indices().refresh(new
		 * RefreshRequest(INDEX_NAME)) .actionGet();
		 * 
		 * GetResponse response = node.client() .prepareGet(INDEX_NAME,
		 * INDEX_TYPE, String.valueOf(id)) .execute().actionGet(); boolean exist
		 * = response.exists(); logger.info("Document by id exist? " + exist);
		 * 
		 * Assert.assertTrue(exist);
		 * 
		 * SearchRequestBuilder srb; SearchResponse searchResponse;
		 * 
		 * // QueryBuilder qb1 = termQuery("content._name", filename.getName());
		 * // // SearchRequestBuilder srb =
		 * client.prepareSearch(INDEX_NAME).setQuery( // qb1); // //
		 * SearchResponse searchResponse = srb.execute().actionGet(); // //
		 * logger.info(searchResponse.toString()); // //
		 * Assert.assertTrue(searchResponse.getHits().getTotalHits() == 1);
		 * 
		 * QueryStringQueryBuilder qb =
		 * QueryBuilders.queryString("introduction");
		 * 
		 * logger.info(qb.toString());
		 * 
		 * srb = node.client().prepareSearch(INDEX_NAME).setQuery(qb);
		 * 
		 * searchResponse = srb.execute().actionGet();
		 * 
		 * logger.info(searchResponse.toString());
		 * 
		 * logger.info("Total hits: {}",
		 * searchResponse.getHits().getTotalHits());
		 * Assert.assertTrue(searchResponse.getHits().getTotalHits() == 1); }
		 * 
		 * private XContentBuilder getMapping() throws Throwable {
		 * XContentBuilder mapping = jsonBuilder() .startObject()
		 * .startObject(INDEX_TYPE) .startObject("properties")
		 * .startObject("content").field("type", "attachment").endObject()
		 * .startObject("filename").field("type", "string").endObject()
		 * .startObject("contentType").field("type", "string").endObject()
		 * .startObject("md5").field("type", "string").endObject() //
		 * .startObject("metadata") // .field("type", "object") //
		 * .startObject("properties") // .startObject("firstName").field("type",
		 * "string").endObject() // .startObject("lastName").field("type",
		 * "string").endObject() // .endObject() // .endObject() .endObject()
		 * .endObject() .endObject(); logger.info("Mapping: " +
		 * mapping.string()); return mapping;
		 */
	}

	private byte[] getBytesFromFile(File file) throws IOException {
		InputStream is = new FileInputStream(file);

		// Get the size of the file
		long length = file.length();

		if (length > Integer.MAX_VALUE) {
			// File is too large
		}

		// Create the byte array to hold the data
		byte[] bytes = new byte[(int) length];

		// Read in the bytes
		int offset = 0;
		int numRead = 0;
		while (offset < bytes.length
				&& (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
			offset += numRead;
		}

		// Ensure all the bytes have been read in
		if (offset < bytes.length) {
			throw new IOException("Could not completely read file "
					+ file.getName());
		}

		// Close the input stream and return bytes
		is.close();
		return bytes;
	}
}
