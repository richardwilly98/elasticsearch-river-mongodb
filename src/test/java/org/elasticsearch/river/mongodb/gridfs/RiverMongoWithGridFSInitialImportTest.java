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
package org.elasticsearch.river.mongodb.gridfs;

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.bson.types.ObjectId;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class RiverMongoWithGridFSInitialImportTest extends RiverMongoGridFSTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;

    // @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(getCollection(), new BasicDBObject());
            Assert.assertNotNull(mongoCollection);
            Thread.sleep(wait);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    private void createRiver() throws Exception {
        super.createRiver(TEST_MONGODB_RIVER_GRIDFS_JSON);
        Thread.sleep(wait);
    }

    // @AfterClass
    public void cleanUp() {
        super.deleteRiver();
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @Test
    public void testImportAttachmentInitialImport() throws Exception {
        logger.debug("*** testImportAttachmentInitialImport ***");
        try {
            createDatabase();
            byte[] content = copyToBytesFromClasspath(RiverMongoWithGridFSTest.TEST_ATTACHMENT_HTML);
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

            createRiver();
            Thread.sleep(wait);
            refreshIndex();

            CountResponse countResponse = getNode().client().count(countRequest(getIndex())).actionGet();
            logger.debug("Index total count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(1l));

            GetResponse getResponse = getNode().client().get(getRequest(getIndex()).id(id)).get();
            logger.debug("Get request for id {}: {}", id, getResponse.isExists());
            assertThat(getResponse.isExists(), equalTo(true));

            SearchResponse response = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString("Aliquam")).execute()
                    .actionGet();
            logger.debug("SearchResponse {}", response.toString());
            long totalHits = response.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            in = gridFS.createFile(content);
            in.setFilename("test-attachment-2.html");
            in.setContentType("text/html");
            in.save();
            in.validate();

            id = in.getId().toString();

            out = gridFS.findOne(in.getFilename());
            logger.debug("GridFS from findOne: {}", out);
            out = gridFS.findOne(new ObjectId(id));
            logger.debug("GridFS from findOne: {}", out);
            Assert.assertEquals(out.getId(), in.getId());

            Thread.sleep(wait);
            refreshIndex();

            countResponse = getNode().client().count(countRequest(getIndex())).actionGet();
            logger.debug("Index total count: {}", countResponse.getCount());
            assertThat(countResponse.getCount(), equalTo(2l));

            getResponse = getNode().client().get(getRequest(getIndex()).id(id)).get();
            logger.debug("Get request for id {}: {}", id, getResponse.isExists());
            assertThat(getResponse.isExists(), equalTo(true));

//            countResponse = getNode().client().count(countRequest(getIndex()).query(fieldQuery("_id", id))).actionGet();
//            logger.debug("Index count for id {}: {}", id, countResponse.getCount());
//            assertThat(countResponse.getCount(), equalTo(1l));

            response = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString("Aliquam")).execute().actionGet();
            logger.debug("SearchResponse {}", response.toString());
            totalHits = response.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(2l));

            DBCursor cursor = gridFS.getFileList();
            try {
                while (cursor.hasNext()) {
                    DBObject object = cursor.next();
                    gridFS.remove(new ObjectId(object.get("_id").toString()));
                }
            } finally {
                cursor.close();
            }

            Thread.sleep(wait);
            refreshIndex();

            getResponse = getNode().client().get(getRequest(getIndex()).id(id)).get();
            logger.debug("Get request for id {}: {}", id, getResponse.isExists());
            assertThat(getResponse.isExists(), equalTo(false));
//            countResponse = getNode().client().count(countRequest(getIndex()).query(fieldQuery("_id", id))).actionGet();
//            logger.debug("Count after delete request: {}", countResponse.getCount());
//            assertThat(countResponse.getCount(), equalTo(0L));
        } catch (Throwable t) {
            logger.error("testImportAttachmentInitialImport failed.", t);
            Assert.fail("testImportAttachmentInitialImport failed", t);
        } finally {
            cleanUp();
        }
    }

}
