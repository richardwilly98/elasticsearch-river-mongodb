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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ImportAttachmentTest {

	private final ESLogger logger = Loggers.getLogger(getClass());

	private Client client;
	private String INDEX_NAME = "testindex";
	private String INDEX_TYPE = "testattachment";

	private File filename = new File("resources/test-document.pdf");
	private String contentType = "application/pdf";

	@BeforeClass
	public void beforeClass() {
		logger.info("Initiate client.");
		// String cluster;
		Settings s = ImmutableSettings.settingsBuilder()
		// .put("cluster.name", cluster)
				.build();
		client = new TransportClient(s);
		((TransportClient) client)
				.addTransportAddress(new InetSocketTransportAddress(
						"localhost", 9300));

		try {
			logger.info("Creating index [{}]", INDEX_NAME);
			client.admin().indices().create(new CreateIndexRequest(INDEX_NAME))
					.actionGet();

		} catch (Exception ex) {
			logger.warn("already exists", ex);
		}

		try {
			client.admin().indices().preparePutMapping(INDEX_NAME)
			.setType(INDEX_TYPE).setSource(getMapping()).execute()
			.actionGet();
		} catch (ElasticSearchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@AfterClass
	public void afterClass() {
//		logger.info("Deleting index [{}]", INDEX_NAME);
//		client.admin().indices().delete(new DeleteIndexRequest(INDEX_NAME))
//				.actionGet();
	}

	@Test
	public void importAttachment() {
		String id = "1";
		try {
			byte[] content = getBytesFromFile(filename);
			String encodedContent = Base64.encodeBytes(content);
			XContentBuilder b = jsonBuilder().startObject();
				b.startObject("content");
					b.field("content_type", contentType);
					b.field("title", filename.getName());
					b.field("author", "pippo");
					b.field("content", encodedContent);
				b.endObject();
				b.field("filename", filename.getName());
				b.field("contentType", contentType);
				b.startObject("metadata");
					b.field("firstName", "pippo");
					b.field("lastName", "PLUTO");
				b.endObject();
			b.endObject();
			IndexRequestBuilder irb = client.prepareIndex(INDEX_NAME,
					INDEX_TYPE).setSource(b);
//			IndexRequestBuilder irb = client.prepareIndex(INDEX_NAME,
//					INDEX_TYPE, id).setSource(b);
			IndexResponse indexResponse = irb.execute().actionGet();
			logger.info("Index response: {} - {} - {}", indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId());
			id = indexResponse.getId();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		client.admin().indices().refresh(new RefreshRequest(INDEX_NAME))
				.actionGet();

		GetResponse response = client
				.prepareGet(INDEX_NAME, INDEX_TYPE, String.valueOf(id))
				.execute().actionGet();
		boolean exist = response.exists();
		logger.info("Document by id exist? " + exist);

		Assert.assertTrue(exist);

		SearchRequestBuilder srb;
		SearchResponse searchResponse;
		
//		QueryBuilder qb1 = termQuery("content._name", filename.getName());
//
//		SearchRequestBuilder srb = client.prepareSearch(INDEX_NAME).setQuery(
//				qb1);
//
//		SearchResponse searchResponse = srb.execute().actionGet();
//
//		logger.info(searchResponse.toString());
//
//		Assert.assertTrue(searchResponse.getHits().getTotalHits() == 1);

		 QueryStringQueryBuilder qb = QueryBuilders.queryString("introduction");
		
		 logger.info(qb.toString());
		
		 srb = client.prepareSearch(INDEX_NAME).setQuery(qb);
		
		 searchResponse = srb.execute().actionGet();
		
		 logger.info(searchResponse.toString());
		
		 logger.info("Total hits: {}", searchResponse.getHits().getTotalHits());
		 Assert.assertTrue(searchResponse.getHits().getTotalHits() == 1);
	}

	private XContentBuilder getMapping() throws Throwable {
		XContentBuilder mapping = jsonBuilder()
				.startObject()
					.startObject(INDEX_TYPE)
						.startObject("properties")
							.startObject("content").field("type", "attachment").endObject()
							.startObject("filename").field("type", "string").endObject()
							.startObject("contentType").field("type", "string").endObject()
							.startObject("md5").field("type", "string").endObject()
//							.startObject("metadata")
//								.field("type", "object")
//								.startObject("properties")
//									.startObject("firstName").field("type", "string").endObject()
//									.startObject("lastName").field("type", "string").endObject()
//								.endObject()
//							.endObject()
						.endObject()
					.endObject()
				.endObject();
		logger.info("Mapping: " + mapping.string());
		return mapping;
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
