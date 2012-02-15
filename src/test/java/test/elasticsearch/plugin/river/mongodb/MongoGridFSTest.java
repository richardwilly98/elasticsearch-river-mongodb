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

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/*
 *
 */
@Test
public class MongoGridFSTest {

  private final ESLogger logger = Loggers.getLogger(getClass());
  private final String INDEX_NAME = "testindex";
  private final String TYPE_NAME = "testattachment";

  private Node node;

  @BeforeClass
  public void setupServer() {
    node = NodeBuilder
        .nodeBuilder()
        .local(true)
        .settings(
            ImmutableSettings
            .settingsBuilder()
            .put("path.data", "target/data")
            .put("path.plugins", "target/plugins")
            .put("path.conf", "target/config")
            .put("path.logs", "target/log")
            .put("cluster.name",
                "test-cluster-"
                    + NetworkUtils
                    .getLocalAddress())
                    .put("gateway.type", "none")).node();

  }

  @AfterClass
  public void closeServer() {
    node.close();
  }

  @BeforeMethod
  public void createIndex() throws RuntimeException, Throwable {
    logger.info("creating index [{}]", INDEX_NAME);
    node.client()
    .admin()
    .indices()
    .create(new CreateIndexRequest(INDEX_NAME)
    .settings(ImmutableSettings.settingsBuilder().put(
        "index.numberOfReplicas", 0))).actionGet();
    XContentBuilder mapping = getMapping();
    node.client().admin().indices().preparePutMapping(INDEX_NAME)
    .setType(TYPE_NAME).setSource(mapping).execute().actionGet();

    XContentBuilder settings = getRiverSettings();
    if (settings == null) {
      throw new Exception("Must provide river settings...");
    }
    node.client().prepareIndex("_river", "testmongodb", "_meta")
    .setSource(settings).execute().actionGet();

    // logger.info("Running Cluster Health");
    // ClusterHealthResponse clusterHealth =
    // node.client().admin().cluster().health(Requests.clusterHealthRequest().waitForGreenStatus()).actionGet();
    // logger.info("Done Cluster Health, status " + clusterHealth.status());
    // MatcherAssert.assertThat(clusterHealth.timedOut(),
    // Matchers.equalTo(false));
    // MatcherAssert.assertThat(clusterHealth.status(),
    // Matchers.equalTo(ClusterHealthStatus.GREEN));
  }

  @AfterMethod
  public void deleteIndex() {
    logger.info("deleting index [{}]", INDEX_NAME);
    node.client().admin().indices()
    .delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
  }

  @Test
  public void testGridFS() throws Throwable {
    logger.info("Start testGridFS");
    // Let's wait one hour
    Thread.sleep(5 * 60 * 1000);
  }

  public XContentBuilder getMapping() throws Throwable {
    XContentBuilder mapping = jsonBuilder().startObject()
        .startObject(TYPE_NAME)
        .startObject("properties")
        .startObject("content")
        .field("type", "attachment")
        .endObject()
        .startObject("filename")
        .field("type", "string")
        .endObject()
        .startObject("contentType")
        .field("type", "string")
        .endObject()
        .startObject("md5")
        .field("type", "string")
        .endObject()
        .endObject()
        .endObject().endObject();
    logger.debug("Mapping: " + mapping.string());
    return mapping;
  }

  public XContentBuilder getRiverSettings() throws Exception {
    XContentBuilder xb = jsonBuilder().startObject()
        .field("type", MongoDBRiver.RIVER_TYPE)
        .startObject(MongoDBRiver.RIVER_TYPE)
        .field(MongoDBRiver.DB_FIELD, "test")
        .field(MongoDBRiver.COLLECTION_FIELD, "fs")
        .endObject()
        .startObject(MongoDBRiver.INDEX_OBJECT)
        .field(MongoDBRiver.NAME_FIELD, INDEX_NAME)
        .field(MongoDBRiver.TYPE_FIELD, TYPE_NAME)
        .endObject()
        .endObject();
    logger.debug("River settings: " + xb.string());
    return xb;
  }
}
