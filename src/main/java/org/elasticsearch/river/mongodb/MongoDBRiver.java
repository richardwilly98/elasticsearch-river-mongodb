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
package org.elasticsearch.river.mongodb;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.util.concurrent.jsr166y.TransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.mongodb.util.GridFSHelper;
import org.elasticsearch.script.ScriptService;

import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.util.JSON;

/**
 * @author richardwilly98 (Richard Louapre)
 * @author flaper87 (Flavio Percoco Premoli)
 * @author aparo (Alberto Paro)
 * @author kryptt (Rodolfo Hansen)
 */
public class MongoDBRiver extends AbstractRiverComponent implements River {

  public final static String RIVER_TYPE = "mongodb";
  public final static String ROOT_NAME = RIVER_TYPE;
  public final static String DB_FIELD = "db";
  public final static String HOST_FIELD = "host";
  public final static String PORT_FIELD = "port";
  public final static String FILTER_FIELD = "filter";
  public final static String PASSWORD_FIELD = "password";
  public final static String USER_FIELD = "user";
  public final static String SCRIPT_FIELD = "script";
  public final static String COLLECTION_FIELD = "collection";
  public final static String GRIDFS_FIELD = "gridfs";
  public final static String INDEX_OBJECT = "index";
  public final static String NAME_FIELD = "name";
  public final static String TYPE_FIELD = "type";
  public final static String BULK_SIZE_FIELD = "bulk_size";
  public final static String BULK_TIMEOUT_FIELD = "bulk_timeout";
  public final static String LAST_TIMESTAMP_FIELD = "_last_ts";
  public final static String MONGODB_LOCAL = "local";
  public final static String OPLOG_COLLECTION = "oplog.rs";
  public final static String OPLOG_NAMESPACE = "ns";
  public final static String OPLOG_OBJECT = "o";
  public final static String OPLOG_OPERATION = "op";
  public final static String OPLOG_TIMESTAMP = "ts";

  protected final Client client;

  protected final String riverIndexName;

  protected final String mongoHost;
  protected final int mongoPort;
  protected final String mongoDb;
  protected final String mongoCollection;
  protected final boolean mongoGridFS;
  protected final String mongoUser;
  protected final String mongoPassword;
  protected final String mongoOplogNamespace;


  protected final String indexName;
  protected final String typeName;
  protected final int bulkSize;
  protected final TimeValue bulkTimeout;

  protected Thread tailerThread;
  protected Thread indexerThread;
  protected volatile boolean active = true;

  private final TransferQueue<Map<String, Object>> stream = new LinkedTransferQueue<Map<String, Object>>();

  @SuppressWarnings("unchecked")
  @Inject
  public MongoDBRiver(final RiverName riverName, final RiverSettings settings,
      @RiverIndexName final String riverIndexName, final Client client, final ScriptService scriptService) {
    super(riverName, settings);
    this.riverIndexName = riverIndexName;
    this.client = client;

    if (settings.settings().containsKey(RIVER_TYPE)) {
      Map<String, Object> mongoSettings = (Map<String, Object>) settings.settings().get(RIVER_TYPE);
      mongoHost = XContentMapValues.nodeStringValue(mongoSettings.get(HOST_FIELD), "localhost");
      mongoPort = XContentMapValues.nodeIntegerValue(mongoSettings.get(PORT_FIELD), 27017);
      mongoDb = XContentMapValues.nodeStringValue(mongoSettings.get(DB_FIELD), riverName.name());
      mongoCollection = XContentMapValues.nodeStringValue(mongoSettings.get(COLLECTION_FIELD), riverName.name());
      mongoGridFS = XContentMapValues.nodeBooleanValue(mongoSettings.get(GRIDFS_FIELD), false);
      if (mongoSettings.containsKey(USER_FIELD) && mongoSettings.containsKey(PASSWORD_FIELD)) {
        mongoUser = mongoSettings.get(USER_FIELD).toString();
        mongoPassword = mongoSettings.get(PASSWORD_FIELD).toString();
      } else {
        mongoUser = "";
        mongoPassword = "";
      }
    } else {
      mongoHost = "localhost";
      mongoPort = 27017;
      mongoDb = riverName.name();
      mongoCollection = riverName.name();
      mongoGridFS = false;
      mongoUser = "";
      mongoPassword = "";
    }
    mongoOplogNamespace = mongoDb + "." + mongoCollection;

    if (settings.settings().containsKey(INDEX_OBJECT)) {
      Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get(INDEX_OBJECT);
      indexName = XContentMapValues.nodeStringValue(indexSettings.get(NAME_FIELD), mongoDb);
      typeName = XContentMapValues.nodeStringValue(indexSettings.get(TYPE_FIELD), mongoDb);
      bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE_FIELD), 100);
      if (indexSettings.containsKey(BULK_TIMEOUT_FIELD)) {
        bulkTimeout =
            TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get(BULK_TIMEOUT_FIELD), "10ms"),
                TimeValue.timeValueMillis(10));
      } else {
        bulkTimeout = TimeValue.timeValueMillis(10);
      }
    } else {
      indexName = mongoDb;
      typeName = mongoDb;
      bulkSize = 100;
      bulkTimeout = TimeValue.timeValueMillis(10);
    }
  }

  @Override
  public void start() {
    logger.info(
        "starting mongodb stream: host [{}], port [{}], gridfs [{}], filter [{}], db [{}], indexing to [{}]/[{}]",
        mongoHost, mongoPort, mongoGridFS, mongoDb, indexName, typeName);
    try {
      client.admin().indices().prepareCreate(indexName).execute().actionGet();
    } catch (Exception e) {
      if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
        // that's fine
      } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
        // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
        // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the
        // block is removed...
      } else {
        logger.warn("failed to create index [{}], disabling river...",
            e, indexName);
        return;
      }
    }

    if (mongoGridFS) {
      try {
        client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(getGridFSMapping())
        .execute().actionGet();
      } catch (Exception e) {
        logger.warn("Failed to set explicit mapping (attachment): {}", e);
        if (logger.isDebugEnabled()) {
          logger.debug("Set explicit attachment mapping.", e);
        }
      }
    }

    tailerThread =
        EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_slurper").newThread(new Slurper());
    indexerThread =
        EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_indexer").newThread(new Indexer());
    indexerThread.start();
    tailerThread.start();
  }

  @Override
  public void close() {
    if (active) {
      logger.info("closing mongodb stream river");
      active = false;
      tailerThread.interrupt();
      indexerThread.interrupt();
    }
  }

  private class Indexer implements Runnable {

    @Override
    public void run() {
      while (active) {
        try {
          BSONTimestamp lastTimestamp = null;
          BulkRequestBuilder bulk = client.prepareBulk();

          // 1. Attempt to fill as much of the bulk request as possible
          Map<String, Object> data = stream.take();
          lastTimestamp = updateBulkRequest(bulk, data);
          while ((data = stream.poll(bulkTimeout.millis(), MILLISECONDS)) != null) {
            lastTimestamp = updateBulkRequest(bulk, data);
            if (bulk.numberOfActions() >= bulkSize) {
              break;
            }
          }

          // 2. Update the timestamp
          if (lastTimestamp != null) {
            updateLastTimestamp(mongoOplogNamespace, lastTimestamp, bulk);
          }

          // 3. Execute the bulk requests
          try {
            BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
              // TODO write to exception queue?
              logger.warn("failed to execute" + response.buildFailureMessage());
            }
          } catch (Exception e) {
            logger.warn("failed to execute bulk", e);
          }

        } catch (InterruptedException e) {
          logger.debug("river-mongodb indexer interrupted");
        }
      }
    }

    private BSONTimestamp updateBulkRequest(final BulkRequestBuilder bulk, final Map<String, Object> data) {
      if (data.get("_id") == null) {
        logger.warn("Cannot get object id. Skip the current item: [{}]", data);
        return null;
      }
      BSONTimestamp lastTimestamp = (BSONTimestamp) data.get(OPLOG_TIMESTAMP);
      String operation = data.get(OPLOG_OPERATION).toString();
      String objectId = data.get("_id").toString();
      data.remove(OPLOG_TIMESTAMP);
      data.remove(OPLOG_OPERATION);
      try {
        if ("i".equals(operation) || "u".equals(operation)) {
          logger.debug("Operation: {} - id: {} - contains attachment: {}", operation, objectId,
              data.containsKey("attachment"));
          bulk.add(indexRequest(indexName).type(typeName).id(objectId).source(build(data, objectId)));
        }
        if ("d".equals(operation)) {
          logger.info("Delete request [{}], [{}], [{}]", indexName, typeName, objectId);
          bulk.add(new DeleteRequest(indexName, typeName, objectId));
        }
      } catch (IOException e) {
        logger.warn("failed to parse {}", e, data);
      }
      return lastTimestamp;
    }

    private XContentBuilder build(final Map<String, Object> data, final String objectId) throws IOException {
      if (data.containsKey("attachment")) {
        logger.info("Add Attachment: {} to index {} / type {}", objectId, indexName, typeName);
        return GridFSHelper.serialize((GridFSDBFile) data.get("attachment"));
      }
      else {
        return XContentFactory.jsonBuilder().map(data);
      }
    }
  }

  private class Slurper implements Runnable {

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      // There should be just one Mongo instance per jvm
      // it has an internal connection pooling. In this case
      // we'll use a single Mongo to handle the river.
      Mongo mongo = null;
      try {
        ArrayList<ServerAddress> addr = new ArrayList<ServerAddress>();
        addr.add(new ServerAddress(mongoHost, mongoPort));
        mongo = new Mongo(addr);
        // Only the master contains local/oplog collection
        // m.slaveOk();
      } catch (UnknownHostException e) {
        e.printStackTrace(); // To change body of catch statement use
        // File | Settings | File Templates.
      }

      while (active) {
        try {
          DB local = mongo.getDB(MONGODB_LOCAL);
          if (!mongoUser.isEmpty() && !mongoPassword.isEmpty()) {
            boolean auth = local.authenticate(mongoUser,
                mongoPassword.toCharArray());
            if (auth == false) {
              logger.warn("Invalid credential");
              break;
            }
          }

          DBCollection coll = local.getCollection(OPLOG_COLLECTION);
          DBCursor cur = coll.find(getIndexFilter(mongoOplogNamespace))
              .sort(new BasicDBObject("$natural", 1))
              .addOption(Bytes.QUERYOPTION_TAILABLE)
              .addOption(Bytes.QUERYOPTION_AWAITDATA);

          DBObject item;
          while ((item = cur.next()) != null) {
            String operation = item.get(OPLOG_OPERATION).toString();
            String namespace = item.get(OPLOG_NAMESPACE).toString();
            BSONTimestamp currentTimestamp = (BSONTimestamp) item.get(OPLOG_TIMESTAMP);
            DBObject object = (DBObject) item.get(OPLOG_NAMESPACE);
            if (namespace.startsWith(mongoOplogNamespace)) {

              // Not interested by chunks - skip all them
              if (namespace.endsWith(".chunks")) {
                continue;
              }

              logger.trace("oplog processing item {}", item);

              if (mongoGridFS && namespace.endsWith(".files") && ("i".equals(operation) || "u".equals(operation))) {
                String objectId = object.get("_id").toString();
                GridFS grid = new GridFS(mongo.getDB(mongoDb), mongoCollection);
                GridFSDBFile file = grid.findOne(new ObjectId(objectId));
                if (file != null) {
                  logger.info("Caught file: {} - {}", file.getId(), file.getFilename());
                  object = file;
                } else {
                  logger.warn("Cannot find file from id: {}", objectId);
                }
              }

              Map<String, Object> data;
              if (object instanceof GridFSDBFile) {
                logger.info("Add attachment: {}", object.get("_id"));
                data = new HashMap<String, Object>();
                data.put("attachment", object);
              }
              else {
                data = object.toMap();
              }

              data.put("_id", object.get("_id"));
              data.put(OPLOG_TIMESTAMP, currentTimestamp);
              data.put(OPLOG_OPERATION, operation);

              stream.add(data);
            } else {
              logger.debug("Skip namespace: {}", namespace);
            }
          }
          Thread.sleep(bulkTimeout.getMillis()); // sleep since mongo seems to have dropped the cursor.
        } catch (MongoException mEx) {
          logger.error("Mongo gave an exception", mEx);
        } catch (InterruptedException e) {
          logger.debug("river-mongodb slurper interrupted");
        }
      }
    }

    private DBObject getIndexFilter(final String namespace) {
      BSONTimestamp time = getLastTimestamp(namespace);
      DBObject filter = null;
      if (time != null) {
        filter = new BasicDBObject();
        filter.put(OPLOG_TIMESTAMP, new BasicDBObject("$gt", time));
        if (logger.isDebugEnabled()) {
          logger.debug("Using filter: {}", filter);
        }
      } else {
        logger.info("filter is null.");
      }
      return filter;
    }
  }

  private XContentBuilder getGridFSMapping() throws IOException {
    XContentBuilder mapping = jsonBuilder()
        .startObject()
        .startObject(typeName)
        .startObject("properties")
        .startObject("content").field("type", "attachment").endObject()
        .startObject("filename").field("type", "string").endObject()
        .startObject("contentType").field("type", "string").endObject()
        .startObject("md5").field("type", "string").endObject()
        .startObject("length").field("type", "long").endObject()
        .startObject("chunkSize").field("type", "long").endObject()
        .endObject()
        .endObject()
        .endObject();
    logger.info("Mapping: {}", mapping.string());
    return mapping;
  }

  /**
   * Get the latest timestamp for a given namespace.
   */
  @SuppressWarnings("unchecked")
  private BSONTimestamp getLastTimestamp(final String namespace) {
    GetResponse lastTimestampResponse =
        client.prepareGet(riverIndexName, riverName.getName(), namespace).execute().actionGet();
    if (lastTimestampResponse.exists()) {
      Map<String, Object> mongodbState = (Map<String, Object>) lastTimestampResponse.sourceAsMap().get(ROOT_NAME);
      if (mongodbState != null) {
        String lastTimestamp = mongodbState.get(LAST_TIMESTAMP_FIELD).toString();
        if (lastTimestamp != null) {
          logger.debug("{} last timestamp: {}", namespace, lastTimestamp);
          return (BSONTimestamp) JSON.parse(lastTimestamp);

        }
      }
    }
    return null;
  }

  /**
   * Adds an index request operation to a bulk request,
   * updating the last timestamp for a given namespace (ie: host:dbName.collectionName)
   * @param bulk
   */
  private void updateLastTimestamp(final String namespace, final BSONTimestamp time, final BulkRequestBuilder bulk) {
    try {
      bulk.add(indexRequest(riverIndexName).type(riverName.getName()).id(namespace)
          .source(jsonBuilder()
              .startObject()
              .startObject(ROOT_NAME)
              .field(LAST_TIMESTAMP_FIELD, JSON.serialize(time))
              .endObject()
              .endObject()));
    } catch (IOException e) {
      logger.error("error updating last timestamp for namespace {}", namespace);
    }
  }

}
