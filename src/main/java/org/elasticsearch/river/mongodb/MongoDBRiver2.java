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

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.util.concurrent.jsr166y.TransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.mongodb.util.GridFSHelper;
import org.elasticsearch.script.ScriptService;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.util.JSON;

/**
 * @author richardwilly98 (Richard Louapre)
 * @author flaper87 (Flavio Percoco Premoli)
 * @author aparo (Alberto Paro)
 */

public class MongoDBRiver2 extends MongoDBRiver {

	public final static String mongoDBrootName = "mongodb";
	public final static String lastTimestampName = "_last_ts";
	protected final String _namespace;
	public final static String mongoDBLocal = "local";
	public final static String oplogCollection = "oplog.rs";
	public final static String oplogOperation = "op";
	public final static String oplogNamespace = "ns";
	public final static String oplogObject = "o";
	public final static String oplogTimestamp = "ts";

	private final TransferQueue<Map<String, Object>> stream = new LinkedTransferQueue<Map<String, Object>>();

	@Inject
	public MongoDBRiver2(RiverName riverName, RiverSettings settings,
			@RiverIndexName String riverIndexName, Client client,
			ScriptService scriptService) {
		super(riverName, settings, riverIndexName, client, scriptService);
		_namespace = mongoDb + "." + mongoCollection;
		logger.info("river name [{}] monitoring namespace [{}]", riverName,
				_namespace);
	}

	@Override
	public void start() {
		logger.info(
				"starting mongodb stream: host [{}], port [{}], gridfs [{}], filter [{}], db [{}], indexing to [{}]/[{}]",
				mongoHost, mongoPort, mongoGridFS, mongoFilter, mongoDb, indexName, typeName);
		try {
			client.admin().indices().prepareCreate(indexName).execute()
					.actionGet();
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// that's fine
			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ok, not recovered yet..., lets start indexing and hope we
				// recover by the first bulk
				// TODO: a smarter logic can be to register for cluster event
				// listener here, and only start sampling when the block is
				// removed...
			} else {
				logger.warn("failed to create index [{}], disabling river...",
						e, indexName);
				return;
			}

		}

		if (mongoGridFS) {
			try {
				client.admin().indices().preparePutMapping(indexName)
				.setType(typeName).setSource(getMapping()).execute()
				.actionGet();
			} catch (Exception e) {
				logger.warn("Failed to set explicit mapping (attachment): {}", e);
				if (logger.isDebugEnabled()) {
					logger.debug("Set explicit attachment mapping.", e);
				}
			}
			
		}

		tailerThread = EsExecutors.daemonThreadFactory(
				settings.globalSettings(), "mongodb_river_slurper").newThread(
				new Tailer());
		indexerThread = EsExecutors.daemonThreadFactory(
				settings.globalSettings(), "mongodb_river_indexer").newThread(
				new Indexer());
		indexerThread.start();
		tailerThread.start();
	}

	/*
	 * Update last timestamp for a given namespace (ie: dbName.collectionName)
	 */
	private void updateLastTimestamp(String namespace, BSONTimestamp time) {
		BulkRequestBuilder bulk = client.prepareBulk();
		try {
			bulk.add(indexRequest(riverIndexName)
					.type(riverName.getName())
					.id(namespace)
					.source(jsonBuilder().startObject()
							.startObject(mongoDBrootName)
							.field(lastTimestampName, JSON.serialize(time))
							.endObject().endObject()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			BulkResponse response = bulk.execute().actionGet();
			if (response.hasFailures()) {
				// TODO write to exception queue?
				logger.warn("failed to execute"
						+ response.buildFailureMessage());
			}
		} catch (Exception e) {
			logger.warn("failed to execute bulk", e);
		}
	}

	/*
	 * Get the latest timestamp for a given namespace.
	 */
	private BSONTimestamp getLastTimestamp(String namespace) {
		BSONTimestamp time = null;
		GetResponse lastTimestampResponse = client
				.prepareGet(riverIndexName, riverName.getName(), namespace)
				.execute().actionGet();
		if (lastTimestampResponse.exists()) {
			Map<String, Object> mongodbState = (Map<String, Object>) lastTimestampResponse
					.sourceAsMap().get(mongoDBrootName);
			if (mongodbState != null) {
				String lastTimestamp = mongodbState.get(lastTimestampName)
						.toString();
				if (lastTimestamp != null) {
					logger.debug("Last timestamp: " + lastTimestamp);
					time = (BSONTimestamp) JSON.parse(lastTimestamp);
				}
			}
		}
		return time;
	}

	private DBObject getIndexFilter(String namespace) {
		BSONTimestamp time = getLastTimestamp(namespace);
		DBObject filter = null;
		if (time != null) {
			filter = new BasicDBObject();
			filter.put(oplogTimestamp, new BasicDBObject("$gt", time));
			if (logger.isDebugEnabled()) {
				logger.debug("Using filter: " + filter);
			}
		} else {
			logger.info("filter is null.");
		}
		return filter;
	}

	private XContentBuilder getMapping() throws IOException {
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
//							.startObject("uploadDate").field("type", "string").endObject()
						.endObject()
					.endObject()
				.endObject();
		logger.info("Mapping: " + mapping.string());
		return mapping;
	}

	protected class Indexer implements Runnable {
		@Override
		public void run() {
			logger.info("Start indexer thread.");
			while (true) {
				if (closed) {
					return;
				}

				Map<String, Object> data = null;
				try {
					data = stream.take();
				} catch (InterruptedException e) {
					if (closed) {
						return;
					}
					continue;
				}
				BulkRequestBuilder bulk = client.prepareBulk();
				BSONTimestamp lastTimestamp = null;
				lastTimestamp = updateBulkRequest(bulk, data);

				// spin a bit to see if we can get some more changes
				try {
					while ((data = stream.poll(bulkTimeout.millis(),
							TimeUnit.MILLISECONDS)) != null) {

						lastTimestamp = updateBulkRequest(bulk, data);

						if (bulk.numberOfActions() >= bulkSize) {
							break;
						}
					}
				} catch (InterruptedException e) {
					if (closed) {
						return;
					}
				}

				if (lastTimestamp != null) {
					updateLastTimestamp(_namespace, lastTimestamp);
				}

				try {
					BulkResponse response = bulk.execute().actionGet();
					if (response.hasFailures()) {
						// TODO write to exception queue?
						logger.warn("failed to execute"
								+ response.buildFailureMessage());
					}
				} catch (Exception e) {
					logger.warn("failed to execute bulk", e);
				}
			}
		}

		private BSONTimestamp updateBulkRequest(BulkRequestBuilder bulk,
				Map<String, Object> data) {
			XContentBuilder builder;
			BSONTimestamp lastTimestamp = null;
			String operation = null;
			String objectId = null;
			try {
				lastTimestamp = (BSONTimestamp) data.get(oplogTimestamp);
				data.remove(oplogTimestamp);
				operation = data.get(oplogOperation).toString();
				data.remove(oplogOperation);
				if (data.get("_id") != null) {
					objectId = data.get("_id").toString();
				} else {
					logger.warn(
							"Cannot get object id. Skip the current item: [{}]",
							data);
					return null;
				}
				logger.debug("Operation: {} - id: {} - contains attachment: {}", operation, objectId, data.containsKey("attachment"));
				if ("i".equals(operation) || "u".equals(operation)) {
					if (data.containsKey("attachment")) {
						builder = GridFSHelper.serialize((GridFSDBFile) data.get("attachment"));
						logger.info("Add Attachment: {} to index {} / type {}", objectId, indexName, typeName);
					}
					else {
						builder = XContentFactory.jsonBuilder().map(data);
					}

					bulk.add(indexRequest(indexName).type(typeName)
							.id(objectId).source(builder));
				}
				if ("d".equals(operation)) {
					logger.info("Delete request [{}], [{}], [{}]", indexName,
							typeName, objectId);

					bulk.add(new DeleteRequest(indexName, typeName, objectId));
				}

			} catch (IOException e) {
				logger.warn("failed to parse {}", e, data);
			}
			return lastTimestamp;
		}
	}

	protected class Tailer implements Runnable {
		@Override
		public void run() {

			logger.info("Start tailer thread.");
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

			while (true) {
				if (closed) {
					return;
				}

				try {

					try {
						
						DB local = mongo.getDB(mongoDBLocal);
						if (!mongoUser.isEmpty() && !mongoPassword.isEmpty()) {
							boolean auth = local.authenticate(mongoUser,
									mongoPassword.toCharArray());
							if (auth == false) {
								logger.warn("Invalid credential");
								break;
							}
						}

						DBCollection coll = local.getCollection(oplogCollection);
						DBCursor cur = coll.find(getIndexFilter(_namespace))
								.sort(new BasicDBObject("$natural", 1))
								.addOption(Bytes.QUERYOPTION_TAILABLE)
								.addOption(Bytes.QUERYOPTION_AWAITDATA);

						BSONTimestamp currentTimestamp = null;
						String namespace = null;

						while (cur.hasNext()) {

							DBObject item = cur.next();
							String operation = item.get(oplogOperation).toString();
							String tmpNamespace = item.get(oplogNamespace)
									.toString();
							DBObject object = (DBObject) item.get(oplogObject);
							if (tmpNamespace.startsWith(_namespace)) {

								// Not interested by chunks - skip all them
								if (tmpNamespace.endsWith(".chunks")) {
									continue;
								}

								if (logger.isDebugEnabled()) {
									logger.debug(item.toString());
								}
								namespace = tmpNamespace;
								currentTimestamp = (BSONTimestamp) item
										.get(oplogTimestamp);
								String jsonTime = JSON.serialize(currentTimestamp);
								logger.info("Time: " + currentTimestamp.toString()
										+ " - " + jsonTime);
								if ("i".equals(operation) || "u".equals(operation)) {
									if (mongoGridFS) {
										if (namespace.endsWith(".files")) {
											String objectId = object.get("_id")
													.toString();
											GridFS grid = new GridFS(
													mongo.getDB(mongoDb),
													mongoCollection);
											GridFSDBFile file = grid
													.findOne(new ObjectId(objectId));
											if (file != null) {
												logger.info("Catch file: "
														+ file.getId() + " - "
														+ file.getFilename());
												object = file;
											} else {
												logger.warn(
														"Cannot find file from id: {}",
														objectId);
											}
											// continue;
										}
									}
								}

								if ("d".equals(operation)) {

								}

								Map<String, Object> mydata;
								if (object instanceof GridFSDBFile) {
									logger.info("Add attachment: {}", object.get("_id"));
									mydata = new HashMap<String, Object>();
									mydata.put("attachment", object);
								}
								else {
									mydata = object.toMap();
								}
								mydata.put("_id", object.get("_id"));
								mydata.put(oplogTimestamp, currentTimestamp);
								mydata.put(oplogOperation, operation);

								stream.add(mydata);
							} else {
								if (logger.isDebugEnabled()) {
									logger.debug("Skip namespace: " + tmpNamespace);
								}
							}
						}
					}
					catch (MongoException mEx){
						
					}
					Thread.sleep(5000);

				} catch (InterruptedException e) {
					if (closed) {
						return;
					}
				}

			}
		}
	}

}