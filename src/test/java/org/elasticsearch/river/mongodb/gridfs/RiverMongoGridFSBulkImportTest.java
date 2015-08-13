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

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.bson.types.ObjectId;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class RiverMongoGridFSBulkImportTest extends RiverMongoGridFSTestAbstract {

    private static final String TEST_ATTACHMENT_HTML = "/org/elasticsearch/river/mongodb/gridfs/test-attachment.html";
    private static final String CRITERIA_ATTACHMENT_HTML = "Aliquam";
    private static final String TEST_ATTACHMENT_PDF = "/org/elasticsearch/river/mongodb/gridfs/lorem.pdf";
    private static final String CRITERIA_ATTACHMENT_PDF = "Lorem ipsum dolor";
    private static final String TEST_ATTACHMENT_PDF_2 = "/org/elasticsearch/river/mongodb/gridfs/test-document.pdf";
    private static final String CRITERIA_ATTACHMENT_PDF_2 = "Design and Specifications";

    private enum SampleFile {
        HTML(TEST_ATTACHMENT_HTML, CRITERIA_ATTACHMENT_HTML), PDF(TEST_ATTACHMENT_PDF, CRITERIA_ATTACHMENT_PDF), PDF2(
                TEST_ATTACHMENT_PDF_2, CRITERIA_ATTACHMENT_PDF_2), ;

        private final String filename;
        private final String criteria;

        private SampleFile(String filename, String criteria) {
            this.filename = filename;
            this.criteria = criteria;
        }

        public String getFilename() {
            return filename;
        }

        public String getCriteria() {
            return criteria;
        }
    }

    private DB mongoDB;
    private DBCollection mongoCollection;

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(TEST_MONGODB_RIVER_GRIDFS_JSON);
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(getCollection(), new BasicDBObject());
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

    @Test(enabled = false)
    public void testImportPDFAttachment() throws Exception {
        logger.debug("*** testImportPDFAttachment ***");
        long count = 10000;
        List<String> ids = newArrayList();
        SampleFile attachment = SampleFile.PDF2;
        final byte[] content = copyToBytesFromClasspath(attachment.getFilename());
        logger.debug("Content in bytes: {}", content.length);

        GridFS gridFS = new GridFS(mongoDB);
        for (int i = 0; i < count; i++) {

            GridFSInputFile in = gridFS.createFile(content);
            in.setFilename("lorem-" + i + ".pdf");
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
            ids.add(id);
        }

        CountResponse countResponse;

        while (true) {
            Thread.sleep(wait);
            refreshIndex();
            countResponse = getNode().client().count(countRequest(getIndex())).actionGet();
            logger.debug("Index total count: {}", countResponse.getCount());
            if (countResponse.getCount() == count) {
                break;
            }
        }

        countResponse = getNode().client().count(countRequest(getIndex())).actionGet();
        logger.debug("Index total count: {}", countResponse.getCount());
        assertThat(countResponse.getCount(), equalTo(count));

        SearchResponse response = getNode().client().prepareSearch(getIndex())
                .setQuery(QueryBuilders.queryString(attachment.getCriteria())).execute().actionGet();
        long totalHits = response.getHits().getTotalHits();
        logger.debug("TotalHits: {}", totalHits);
        assertThat(totalHits, equalTo(count));

        for (String id : ids) {
            gridFS.remove(new ObjectId(id));
        }

        Thread.sleep(wait);
        refreshIndex();

        countResponse = getNode().client().count(countRequest(getIndex())).actionGet();
        logger.debug("Count after delete request: {}", countResponse.getCount());
        assertThat(countResponse.getCount(), equalTo(0L));
    }

}
