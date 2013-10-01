package org.elasticsearch.river.mongodb;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.BSONTimestamp;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.MongoDBRiver.QueueEntry;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;

class Indexer implements Runnable {

    private final MongoDBRiverDefinition definition;
    private final SharedContext context;
    private final Client client;
    private final ScriptService scriptService;

    public Indexer(MongoDBRiverDefinition definition, SharedContext context, Client client, ScriptService scriptService) {
        this.definition = definition;
        this.context = context;
        this.client = client;
        this.scriptService = scriptService;
    }

    private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());
    private int deletedDocuments = 0;
    private int insertedDocuments = 0;
    private int updatedDocuments = 0;
    private StopWatch sw;
    private ExecutableScript scriptExecutable;

    @Override
    public void run() {
        while (context.getStatus() == Status.RUNNING) {
            sw = new StopWatch().start();
            deletedDocuments = 0;
            insertedDocuments = 0;
            updatedDocuments = 0;

            if (definition.getScript() != null && definition.getScriptType() != null) {
                scriptExecutable = this.scriptService.executable(definition.getScriptType(), definition.getScript(),
                        ImmutableMap.of("logger", logger));
            }

            try {
                BSONTimestamp lastTimestamp = null;
                BulkRequestBuilder bulk = client.prepareBulk();

                // 1. Attempt to fill as much of the bulk request as possible
                QueueEntry entry = context.getStream().take();
                lastTimestamp = processBlockingQueue(bulk, entry);
                while ((entry = context.getStream().poll(definition.getBulkTimeout().millis(), MILLISECONDS)) != null) {
                    lastTimestamp = processBlockingQueue(bulk, entry);
                    if (bulk.numberOfActions() >= definition.getBulkSize()) {
                        break;
                    }
                }

                // 2. Update the timestamp
                if (lastTimestamp != null) {
                    MongoDBRiver.updateLastTimestamp(definition, lastTimestamp, bulk);
                }

                // 3. Execute the bulk requests
                try {
                    BulkResponse response = bulk.execute().actionGet();
                    if (response.hasFailures()) {
                        // TODO write to exception queue?
                        logger.warn("failed to execute" + response.buildFailureMessage());
                    }
                } catch (ElasticSearchInterruptedException esie) {
                    logger.warn("river-mongodb indexer bas been interrupted", esie);
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    logger.warn("failed to execute bulk", e);
                }

            } catch (InterruptedException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("river-mongodb indexer interrupted");
                }
                Thread.currentThread().interrupt();
                break;
            }
            logStatistics();
        }
    }

    @SuppressWarnings({ "unchecked" })
    private BSONTimestamp processBlockingQueue(final BulkRequestBuilder bulk, QueueEntry entry) {
        if (entry.getData().get(MongoDBRiver.MONGODB_ID_FIELD) == null
                && !entry.getOperation().equals(MongoDBRiver.OPLOG_COMMAND_OPERATION)) {
            logger.warn("Cannot get object id. Skip the current item: [{}]", entry.getData());
            return null;
        }

        BSONTimestamp lastTimestamp = entry.getOplogTimestamp();
        String operation = entry.getOperation();
        if (MongoDBRiver.OPLOG_COMMAND_OPERATION.equals(operation)) {
            try {
                updateBulkRequest(bulk, entry.getData(), null, operation, definition.getIndexName(), definition.getTypeName(), null, null);
            } catch (IOException ioEx) {
                logger.error("Update bulk failed.", ioEx);
            }
            return lastTimestamp;
        }

        String objectId = "";
        if (entry.getData().get(MongoDBRiver.MONGODB_ID_FIELD) != null) {
            objectId = entry.getData().get(MongoDBRiver.MONGODB_ID_FIELD).toString();
        }

        // TODO: Should the river support script filter,
        // advanced_transformation, include_collection for GridFS?
        if (entry.isAttachment()) {
            try {
                updateBulkRequest(bulk, entry.getData(), objectId, operation, definition.getIndexName(), definition.getTypeName(), null,
                        null);
            } catch (IOException ioEx) {
                logger.error("Update bulk failed.", ioEx);
            }
            return lastTimestamp;
        }

        if (scriptExecutable != null && definition.isAdvancedTransformation()) {
            return applyAdvancedTransformation(bulk, entry);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("updateBulkRequest for id: [{}], operation: [{}]", objectId, operation);
        }

        if (!definition.getIncludeCollection().isEmpty()) {
            logger.trace("About to include collection. set attribute {} / {} ", definition.getIncludeCollection(),
                    definition.getMongoCollection());
            entry.getData().put(definition.getIncludeCollection(), definition.getMongoCollection());
        }

        Map<String, Object> ctx = null;
        try {
            ctx = XContentFactory.xContent(XContentType.JSON).createParser("{}").mapAndClose();
        } catch (IOException e) {
            logger.warn("failed to parse {}", e);
        }
        Map<String, Object> data = entry.getData().toMap();
        if (scriptExecutable != null) {
            if (ctx != null) {
                ctx.put("document", entry.getData());
                ctx.put("operation", operation);
                if (!objectId.isEmpty()) {
                    ctx.put("id", objectId);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Script to be executed: {}", scriptExecutable);
                    logger.debug("Context before script executed: {}", ctx);
                }
                scriptExecutable.setNextVar("ctx", ctx);
                try {
                    scriptExecutable.run();
                    // we need to unwrap the context object...
                    ctx = (Map<String, Object>) scriptExecutable.unwrap(ctx);
                } catch (Exception e) {
                    logger.warn("failed to script process {}, ignoring", e, ctx);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Context after script executed: {}", ctx);
                }
                if (ctx.containsKey("ignore") && ctx.get("ignore").equals(Boolean.TRUE)) {
                    logger.debug("From script ignore document id: {}", objectId);
                    // ignore document
                    return lastTimestamp;
                }
                if (ctx.containsKey("deleted") && ctx.get("deleted").equals(Boolean.TRUE)) {
                    ctx.put("operation", MongoDBRiver.OPLOG_DELETE_OPERATION);
                }
                if (ctx.containsKey("document")) {
                    data = (Map<String, Object>) ctx.get("document");
                    logger.debug("From script document: {}", data);
                }
                if (ctx.containsKey("operation")) {
                    operation = ctx.get("operation").toString();
                    logger.debug("From script operation: {}", operation);
                }
            }
        }

        try {
            String index = extractIndex(ctx);
            String type = extractType(ctx);
            String parent = extractParent(ctx);
            String routing = extractRouting(ctx);
            objectId = extractObjectId(ctx, objectId);
            updateBulkRequest(bulk, new BasicDBObject(data), objectId, operation, index, type, routing, parent);
        } catch (IOException e) {
            logger.warn("failed to parse {}", e, entry.getData());
        }
        return lastTimestamp;
    }

    private void updateBulkRequest(final BulkRequestBuilder bulk, DBObject data, String objectId, String operation, String index,
            String type, String routing, String parent) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Operation: {} - index: {} - type: {} - routing: {} - parent: {}", operation, index, type, routing, parent);
        }
        boolean isAttachment = false;

        if (logger.isDebugEnabled()) {
            isAttachment = (data instanceof GridFSDBFile);
        }
        if (MongoDBRiver.OPLOG_INSERT_OPERATION.equals(operation)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Insert operation - id: {} - contains attachment: {}", operation, objectId, isAttachment);
            }
            bulk.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));
            insertedDocuments++;
        }
        if (MongoDBRiver.OPLOG_UPDATE_OPERATION.equals(operation)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Update operation - id: {} - contains attachment: {}", objectId, isAttachment);
            }
            deleteBulkRequest(bulk, objectId, index, type, routing, parent);
            bulk.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));
            updatedDocuments++;
        }
        if (MongoDBRiver.OPLOG_DELETE_OPERATION.equals(operation)) {
            logger.info("Delete request [{}], [{}], [{}]", index, type, objectId);
            deleteBulkRequest(bulk, objectId, index, type, routing, parent);
            deletedDocuments++;
        }
        if (MongoDBRiver.OPLOG_COMMAND_OPERATION.equals(operation)) {
            if (definition.isDropCollection()) {
                if (data.get(MongoDBRiver.OPLOG_DROP_COMMAND_OPERATION) != null
                        && data.get(MongoDBRiver.OPLOG_DROP_COMMAND_OPERATION).equals(definition.getMongoCollection())) {
                    logger.info("Drop collection request [{}], [{}]", index, type);
                    bulk.request().requests().clear();
                    client.admin().indices().prepareRefresh(index).execute().actionGet();
                    Map<String, MappingMetaData> mappings = client.admin().cluster().prepareState().execute().actionGet().getState()
                            .getMetaData().index(index).mappings();
                    logger.trace("mappings contains type {}: {}", type, mappings.containsKey(type));
                    if (mappings.containsKey(type)) {
                        /*
                         * Issue #105 - Mapping changing from custom mapping to
                         * dynamic when drop_collection = true Should capture
                         * the existing mapping metadata (in case it is has been
                         * customized before to delete.
                         */
                        MappingMetaData mapping = mappings.get(type);
                        client.admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet();
                        PutMappingResponse pmr = client.admin().indices().preparePutMapping(index).setType(type)
                                .setSource(mapping.source().string()).execute().actionGet();
                        if (!pmr.isAcknowledged()) {
                            logger.error("Failed to put mapping {} / {} / {}.", index, type, mapping.source());
                        }
                    }

                    deletedDocuments = 0;
                    updatedDocuments = 0;
                    insertedDocuments = 0;
                    logger.info("Delete request for index / type [{}] [{}] successfully executed.", index, type);
                } else {
                    logger.debug("Database command {}", data);
                }
            } else {
                logger.info("Ignore drop collection request [{}], [{}]. The option has been disabled.", index, type);
            }
        }

    }

    /*
     * Delete children when parent / child is used
     */
    private void deleteBulkRequest(BulkRequestBuilder bulk, String objectId, String index, String type, String routing, String parent) {
        logger.trace("bulkDeleteRequest - objectId: {} - index: {} - type: {} - routing: {} - parent: {}", objectId, index, type, routing,
                parent);

        if (definition.getParentTypes() != null && definition.getParentTypes().contains(type)) {
            QueryBuilder builder = QueryBuilders.hasParentQuery(type, QueryBuilders.termQuery(MongoDBRiver.MONGODB_ID_FIELD, objectId));
            SearchResponse response = client.prepareSearch(index).setQuery(builder).setRouting(routing)
                    .addField(MongoDBRiver.MONGODB_ID_FIELD).execute().actionGet();
            for (SearchHit hit : response.getHits().getHits()) {
                bulk.add(deleteRequest(index).type(hit.getType()).id(hit.getId()).routing(routing).parent(objectId));
            }
        }
        bulk.add(deleteRequest(index).type(type).id(objectId).routing(routing).parent(parent));
    }

    @SuppressWarnings("unchecked")
    private BSONTimestamp applyAdvancedTransformation(final BulkRequestBuilder bulk, QueueEntry entry) {

        BSONTimestamp lastTimestamp = entry.getOplogTimestamp();
        String operation = entry.getOperation();
        String objectId = "";
        if (entry.getData().get(MongoDBRiver.MONGODB_ID_FIELD) != null) {
            objectId = entry.getData().get(MongoDBRiver.MONGODB_ID_FIELD).toString();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("advancedUpdateBulkRequest for id: [{}], operation: [{}]", objectId, operation);
        }

        if (!definition.getIncludeCollection().isEmpty()) {
            logger.trace("About to include collection. set attribute {} / {} ", definition.getIncludeCollection(),
                    definition.getMongoCollection());
            entry.getData().put(definition.getIncludeCollection(), definition.getMongoCollection());
        }
        Map<String, Object> ctx = null;
        try {
            ctx = XContentFactory.xContent(XContentType.JSON).createParser("{}").mapAndClose();
        } catch (Exception e) {
        }

        List<Object> documents = new ArrayList<Object>();
        Map<String, Object> document = new HashMap<String, Object>();

        if (scriptExecutable != null) {
            if (ctx != null && documents != null) {

                document.put("data", entry.getData().toMap());
                if (!objectId.isEmpty()) {
                    document.put("id", objectId);
                }
                document.put("_index", definition.getIndexName());
                document.put("_type", definition.getTypeName());
                document.put("operation", operation);

                documents.add(document);

                ctx.put("documents", documents);
                if (logger.isDebugEnabled()) {
                    logger.debug("Script to be executed: {}", scriptExecutable);
                    logger.debug("Context before script executed: {}", ctx);
                }
                scriptExecutable.setNextVar("ctx", ctx);
                try {
                    scriptExecutable.run();
                    // we need to unwrap the context object...
                    ctx = (Map<String, Object>) scriptExecutable.unwrap(ctx);
                } catch (Exception e) {
                    logger.warn("failed to script process {}, ignoring", e, ctx);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Context after script executed: {}", ctx);
                }
                if (ctx.containsKey("documents") && ctx.get("documents") instanceof List<?>) {
                    documents = (List<Object>) ctx.get("documents");
                    for (Object object : documents) {
                        if (object instanceof Map<?, ?>) {
                            Map<String, Object> item = (Map<String, Object>) object;
                            logger.trace("item: {}", item);
                            if (item.containsKey("deleted") && item.get("deleted").equals(Boolean.TRUE)) {
                                item.put("operation", MongoDBRiver.OPLOG_DELETE_OPERATION);
                            }

                            String index = extractIndex(item);
                            String type = extractType(item);
                            String parent = extractParent(item);
                            String routing = extractRouting(item);
                            String action = extractOperation(item);
                            boolean ignore = isDocumentIgnored(item);
                            Map<String, Object> data = (Map<String, Object>) item.get("data");
                            objectId = extractObjectId(data, objectId);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Id: {} - operation: {} - ignore: {} - index: {} - type: {} - routing: {} - parent: {}",
                                        objectId, action, ignore, index, type, routing, parent);
                            }
                            if (ignore) {
                                continue;
                            }
                            try {
                                updateBulkRequest(bulk, new BasicDBObject(data), objectId, operation, index, type, routing, parent);
                            } catch (IOException ioEx) {
                                logger.error("Update bulk failed.", ioEx);
                            }
                        }
                    }
                }
            }
        }

        return lastTimestamp;
    }

    @SuppressWarnings("unchecked")
    private XContentBuilder build(final DBObject data, final String objectId) throws IOException {
        if (data instanceof GridFSDBFile) {
            logger.info("Add Attachment: {} to index {} / type {}", objectId, definition.getIndexName(), definition.getTypeName());
            return MongoDBHelper.serialize((GridFSDBFile) data);
        } else {
            return XContentFactory.jsonBuilder().map(data.toMap());
        }
    }

    private String extractObjectId(Map<String, Object> ctx, String objectId) {
        Object id = ctx.get("id");
        if (id != null) {
            return id.toString();
        }
        id = ctx.get(MongoDBRiver.MONGODB_ID_FIELD);
        if (id != null) {
            return id.toString();
        } else {
            return objectId;
        }
    }

    private String extractParent(Map<String, Object> ctx) {
        Object parent = ctx.get("_parent");
        if (parent == null) {
            return null;
        } else {
            return parent.toString();
        }
    }

    private String extractRouting(Map<String, Object> ctx) {
        Object routing = ctx.get("_routing");
        if (routing == null) {
            return null;
        } else {
            return routing.toString();
        }
    }

    private String extractOperation(Map<String, Object> ctx) {
        Object operation = ctx.get("operation");
        if (operation == null) {
            return null;
        } else {
            return operation.toString();
        }
    }

    private boolean isDocumentIgnored(Map<String, Object> ctx) {
        return (ctx.containsKey("ignore") && ctx.get("ignore").equals(Boolean.TRUE));
    }

    private String extractType(Map<String, Object> ctx) {
        Object type = ctx.get("_type");
        if (type == null) {
            return definition.getTypeName();
        } else {
            return type.toString();
        }
    }

    private String extractIndex(Map<String, Object> ctx) {
        String index = (String) ctx.get("_index");
        if (index == null) {
            index = definition.getIndexName();
        }
        return index;
    }

    private void logStatistics() {
        long totalDocuments = deletedDocuments + insertedDocuments;
        long totalTimeInSeconds = sw.stop().totalTime().seconds();
        long totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? totalDocuments : totalDocuments / totalTimeInSeconds;
        logger.info("Indexed {} documents, {} insertions, {} updates, {} deletions, {} documents per second", totalDocuments,
                insertedDocuments, updatedDocuments, deletedDocuments, totalDocumentsPerSecond);
    }
}