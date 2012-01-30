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

import com.mongodb.*;
import org.bson.types.ObjectId;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.util.concurrent.jsr166y.TransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * @author richardwilly98 (Richard Louapre)
 * @author flaper87 (Flavio Percoco Premoli)
 * @author aparo (Alberto Paro)
 */

public class MongoDBRiver extends AbstractRiverComponent implements River {

	public final static String RIVER_TYPE = "mongodb";
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
	
    protected final Client client;

    protected final String riverIndexName;

    protected final String mongoHost;
    protected final int mongoPort;
    protected final String mongoDb;
    protected final String mongoCollection;
    protected final boolean mongoGridFS;
//    protected final String mongoFilter;
    protected final String mongoUser;
    protected final String mongoPassword;

    protected final String indexName;
    protected final String typeName;
    protected final int bulkSize;
    protected final TimeValue bulkTimeout;

//    protected final ExecutableScript script;
    protected final Map<String, Object> scriptParams = Maps.newHashMap();

    protected volatile Thread tailerThread;
    protected volatile Thread indexerThread;
    protected volatile boolean closed;

    private final TransferQueue<Map> stream = new LinkedTransferQueue<Map>();

    @Inject public MongoDBRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client, ScriptService scriptService) {
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
//            mongoFilter = XContentMapValues.nodeStringValue(mongoSettings.get(FILTER_FIELD), null);
            if (mongoSettings.containsKey(USER_FIELD) && mongoSettings.containsKey(PASSWORD_FIELD)) {
                mongoUser = mongoSettings.get(USER_FIELD).toString();
                mongoPassword = mongoSettings.get(PASSWORD_FIELD).toString();
            }else{
                mongoUser = "";
                mongoPassword = "";
            }

//            if (mongoSettings.containsKey(SCRIPT_FIELD)) {
//                script = scriptService.executable("js", mongoSettings.get(SCRIPT_FIELD).toString(), Maps.newHashMap());
//            } else {
//                script = null;
//            }
        } else {
            mongoHost = "localhost";
            mongoPort = 27017;
            mongoDb = riverName.name();
//            mongoFilter = null;
            mongoCollection = riverName.name();
            mongoGridFS = false;
            mongoUser = "";
            mongoPassword = "";
//            script = null;
        }

        if (settings.settings().containsKey(INDEX_OBJECT)) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get(INDEX_OBJECT);
            indexName = XContentMapValues.nodeStringValue(indexSettings.get(NAME_FIELD), mongoDb);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get(TYPE_FIELD), mongoDb);
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE_FIELD), 100);
            if (indexSettings.containsKey(BULK_TIMEOUT_FIELD)) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get(BULK_TIMEOUT_FIELD), "10ms"), TimeValue.timeValueMillis(10));
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

    @Override public void start() {
        logger.info("starting mongodb stream: host [{}], port [{}], db [{}], indexing to [{}]/[{}]", mongoHost, mongoPort, mongoDb, indexName, typeName);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        tailerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_slurper").newThread(new Tailer());
        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "mongodb_river_indexer").newThread(new Indexer());
        indexerThread.start();
        tailerThread.start();
    }

    @Override public void close() {
        if (closed) {
            return;
        }
        logger.info("closing mongodb stream river");
        tailerThread.interrupt();
        indexerThread.interrupt();
        closed = true;
    }

    private class Indexer implements Runnable {
        @Override public void run() {
            while (true) {
                if (closed) {
                    return;
                }

                Map data = null;
                try {
                    data = stream.take();
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                    continue;
                }
                BulkRequestBuilder bulk = client.prepareBulk();
                XContentBuilder builder;
                String lastId = null;
                try {
                    builder = XContentFactory.jsonBuilder().map(data);
                    bulk.add(indexRequest(indexName).type(typeName).id(data.get("_id").toString()).source(builder));
                    lastId = data.get("_id").toString();
                } catch (IOException e) {
                    logger.warn("failed to parse {}", e, data);
                }

                // spin a bit to see if we can get some more changes
                try {
                    while ((data = stream.poll(bulkTimeout.millis(), TimeUnit.MILLISECONDS)) != null) {
                        try {
                            builder = XContentFactory.jsonBuilder().map(data);
                            bulk.add(indexRequest(indexName).type(typeName).id(data.get("_id").toString()).source(builder));
                            lastId = data.get("_id").toString();
                        } catch (IOException e) {
                            logger.warn("failed to parse {}", e, data);
                        }

                        if (bulk.numberOfActions() >= bulkSize) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                }

                if (lastId != null) {
                    try {
                        bulk.add(indexRequest(riverIndexName).type(riverName.name()).id("_last_id")
                                .source(jsonBuilder().startObject().startObject("mongodb").field("last_id", lastId).endObject().endObject()));
                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }

                try {
                    BulkResponse response = bulk.execute().actionGet();
                    if (response.hasFailures()) {
                        // TODO write to exception queue?
                        logger.warn("failed to execute" + response.buildFailureMessage());
                    }
                } catch (Exception e) {
                    logger.warn("failed to execute bulk", e);
                }
            }
        }
    }


    private class Tailer implements Runnable {
        @Override public void run() {

            // There should be just one Mongo instance per jvm
            // it has an internal connection pooling. In this case
            // we'll use a single Mongo to handle the river.
            Mongo m = null;
            try {
                ArrayList<ServerAddress> addr = new ArrayList<ServerAddress>();
                addr.add(new ServerAddress(mongoHost, mongoPort));
                m = new Mongo(addr);
                m.slaveOk();
            } catch (UnknownHostException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } 
            
            while (true) {
                if (closed) {
                    return;
                }
                
                try {

                    DB db = m.getDB( mongoDb );
                    if (!mongoUser.isEmpty() && !mongoPassword.isEmpty()){
                        boolean auth = db.authenticate(mongoUser, mongoPassword.toCharArray());
                        if (auth==false){
                            logger.warn("Invalid credential");
                            break;
                        }
                    }


                String lastId = null;
                try {
                    client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
                    GetResponse lastIdResponse = client.prepareGet(riverIndexName, riverName().name(), "_last_id").execute().actionGet();
                    if (lastIdResponse.exists()) {
                        Map<String, Object> mongodbState = (Map<String, Object>) lastIdResponse.sourceAsMap().get("mongodb");
                        if (mongodbState != null) {
                            lastId = mongodbState.get("last_id").toString();
                        }
                    }
                } catch (Exception e) {
                    logger.warn("failed to get last_id", e);
                     try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                }

                    DBCollection coll = db.getCollection(mongoCollection); 
                    DBCursor cur = coll.find();

                    if (lastId != null) {
                        // The $gte filter is useful for capped collections. It allow us to initialize the tail.
                        // See docs.
                        cur = coll.find(new BasicDBObject("_id", new BasicDBObject("$gte", new ObjectId(lastId))));
                    }

                    try {

                        // If coll is capped then use a tailable cursor and sort by $natural
                        if (coll.isCapped())
                        {
                            cur = cur.sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE);
                            logger.debug("Collection is capped adding tailable option");
                        } else {
                            cur = cur.sort(new BasicDBObject("_id", 1));
                        }

                        while(cur.hasNext()) {
                            DBObject doc = cur.next();
                            Map mydata;
                            mydata = doc.toMap();
                            mydata.put("_id", mydata.get("_id").toString());

                            // Should we ignore the first record _id == lastId?
                            //if (mydata.get("_id").toString().equals(lastId))
                            //    continue;

                            stream.add(mydata);
                        }
                    } catch (Exception e) {
                      if (closed) {
                          return;
                      }
                    }

                    // If the query doesn't return any results
                    // we should wait at least a second before
                    // doing the next query, Shouldn't we?
                    Thread.sleep(5000);
                    

                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                }

            }
        }
    }}