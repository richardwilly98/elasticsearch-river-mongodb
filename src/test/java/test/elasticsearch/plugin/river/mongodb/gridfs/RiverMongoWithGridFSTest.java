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
package test.elasticsearch.plugin.river.mongodb.gridfs;

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.bson.types.ObjectId;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import test.elasticsearch.plugin.river.mongodb.RiverMongoDBTestAsbtract;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

@Test
public class RiverMongoWithGridFSTest extends RiverMongoDBTestAsbtract {

	private DB mongoDB;
	private DBCollection mongoCollection;

	protected RiverMongoWithGridFSTest() {
		super("testgridfs-" + System.currentTimeMillis(), "testgridfs-"
				+ System.currentTimeMillis(), "fs", "testattachmentindex-"
				+ System.currentTimeMillis());
	}

	@BeforeClass
	public void createDatabase() {
		logger.debug("createDatabase {}", getDatabase());
		try {
			mongoDB = getMongo().getDB(getDatabase());
			mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
			super.createRiver("/test/elasticsearch/plugin/river/mongodb/gridfs/test-gridfs-mongodb-river.json");
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
	public void testImportAttachment() throws Exception {
		logger.debug("*** testImportAttachment ***");
		byte[] content = copyToBytesFromClasspath("/test/elasticsearch/plugin/river/mongodb/gridfs/test-attachment.html");
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

		Thread.sleep(wait);
		refreshIndex();

		CountResponse countResponse = getNode().client()
				.count(countRequest(getIndex())).actionGet();
		logger.debug("Index total count: {}", countResponse.getCount());
		assertThat(countResponse.getCount(), equalTo(1l));

		countResponse = getNode().client()
				.count(countRequest(getIndex()).query(fieldQuery("_id", id)))
				.actionGet();
		logger.debug("Index count for id {}: {}", id, countResponse.getCount());
		assertThat(countResponse.getCount(), equalTo(1l));

		SearchResponse response = getNode().client().prepareSearch(getIndex())
				.setQuery(QueryBuilders.queryString("Aliquam")).execute()
				.actionGet();
		logger.debug("SearchResponse {}", response.toString());
		long totalHits = response.getHits().getTotalHits();
		logger.debug("TotalHits: {}", totalHits);
		assertThat(totalHits, equalTo(1l));

		gridFS.remove(new ObjectId(id));

		Thread.sleep(wait);
		refreshIndex();

		countResponse = getNode().client()
				.count(countRequest(getIndex()).query(fieldQuery("_id", id)))
				.actionGet();
		logger.debug("Count after delete request: {}", countResponse.getCount());
		assertThat(countResponse.getCount(), equalTo(0L));
	}

	@Test
	public void testImportPDFAttachment() throws Exception {
		logger.debug("*** testImportPDFAttachment ***");
		byte[] content = copyToBytesFromClasspath("/test/elasticsearch/plugin/river/mongodb/gridfs/lorem.pdf");
		logger.debug("Content in bytes: {}", content.length);
		GridFS gridFS = new GridFS(mongoDB);
		GridFSInputFile in = gridFS.createFile(content);
		in.setFilename("lorem.pdf");
		in.setContentType("application/pdf");
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

		Thread.sleep(wait);
		refreshIndex();

		CountResponse countResponse = getNode().client()
				.count(countRequest(getIndex())).actionGet();
		logger.debug("Index total count: {}", countResponse.getCount());
		assertThat(countResponse.getCount(), equalTo(1l));

		countResponse = getNode().client()
				.count(countRequest(getIndex()).query(fieldQuery("_id", id)))
				.actionGet();
		logger.debug("Index count for id {}: {}", id, countResponse.getCount());
		assertThat(countResponse.getCount(), equalTo(1l));

		SearchResponse response = getNode().client().prepareSearch(getIndex())
				.setQuery(QueryBuilders.queryString("Lorem ipsum dolor"))
				.execute().actionGet();
		logger.debug("SearchResponse {}", response.toString());
		long totalHits = response.getHits().getTotalHits();
		logger.debug("TotalHits: {}", totalHits);
		assertThat(totalHits, equalTo(1l));

		gridFS.remove(new ObjectId(id));

		Thread.sleep(wait);
		refreshIndex();

		countResponse = getNode().client()
				.count(countRequest(getIndex()).query(fieldQuery("_id", id)))
				.actionGet();
		logger.debug("Count after delete request: {}", countResponse.getCount());
		assertThat(countResponse.getCount(), equalTo(0L));
	}

}
