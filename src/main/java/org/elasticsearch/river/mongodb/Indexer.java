package org.elasticsearch.river.mongodb;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.types.BSONTimestamp;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
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

    private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());
    private AtomicInteger deletedDocuments = new AtomicInteger();
    private AtomicInteger insertedDocuments = new AtomicInteger();
    private AtomicInteger updatedDocuments = new AtomicInteger();
    private StopWatch sw;
    private final MongoDBRiverDefinition definition;
    private final SharedContext context;
    private final Client client;
    private final ScriptService scriptService;
    private final BulkProcessor bulkProcessor;
    private final Map<String, Boolean> DROP_COLLECTION = ImmutableMap.of("dropCollection", Boolean.TRUE);
    private volatile boolean flushBulkProcessor;

    private AtomicLong documentCount = new AtomicLong();

    private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            logger.trace("beforeBulk - new bulk [{}] of [{} items]", executionId, request.numberOfActions());
            if (flushBulkProcessor) {
                logger.info("About to flush bulk request");
                while (flushBulkProcessor) {
                    if (request.requests().size() == 0) {
                        break;
                    }
                    ActionRequest<?> action = request.requests().get(0);
                    if (action instanceof IndexRequest) {
                        Map<String, Object> source = ((IndexRequest) action).sourceAsMap();
                        if (source.equals(DROP_COLLECTION)) {
                            String index = ((IndexRequest) action).index();
                            String type = ((IndexRequest) action).type();

                            try {
                                dropRecreateMapping(index, type);
                                deletedDocuments.set(0);
                                updatedDocuments.set(0);
                                insertedDocuments.set(0);
                                flushBulkProcessor = false;
                            } catch (Throwable t) {
                                logger.error("Drop collection operation failed", t);
                            }
                        }
                    }
                    request.requests().remove(0);
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            if (failure.getClass().equals(ActionRequestValidationException.class)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Ignore ActionRequestValidationException : {}", failure);
                }
            } else {
                logger.error("afterBulk - Bulk request failed: {} - {} - {}", executionId, request, failure);
                context.setStatus(Status.IMPORT_FAILED);
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                logger.error("Bulk processor failed. {}", response.buildFailureMessage());
                context.setStatus(Status.IMPORT_FAILED);
            } else {
                documentCount.addAndGet(response.getItems().length);
                if (logger.isTraceEnabled()) {
                    logger.trace("afterBulk - bulk [{}] success [{} items] [{} ms] total [{}]", executionId, response.getItems().length,
                            response.getTookInMillis(), documentCount.get());
                }
            }
        }
    };

    public Indexer(MongoDBRiverDefinition definition, SharedContext context, Client client, ScriptService scriptService) {
        this.definition = definition;
        this.context = context;
        this.client = client;
        this.scriptService = scriptService;
        this.bulkProcessor = BulkProcessor.builder(client, listener).setBulkActions(definition.getBulk().getBulkActions())
                .setConcurrentRequests(definition.getBulk().getConcurrentRequests())
                .setFlushInterval(definition.getBulk().getFlushInterval()).setBulkSize(definition.getBulk().getBulkSize()).build();
    }

    @Override
    public void run() {
        while (context.getStatus() == Status.RUNNING) {
            sw = new StopWatch().start();
            deletedDocuments.set(0);
            insertedDocuments.set(0);
            updatedDocuments.set(0);

            try {
                BSONTimestamp lastTimestamp = null;

                // 1. Attempt to fill as much of the bulk request as possible
                QueueEntry entry = context.getStream().take();
                lastTimestamp = processBlockingQueue(entry);
                while ((entry = context.getStream().poll(definition.getBulk().getFlushInterval().millis(), MILLISECONDS)) != null) {
                    lastTimestamp = processBlockingQueue(entry);
                }

                // 2. Update the timestamp
                if (lastTimestamp != null) {
                    MongoDBRiver.setLastTimestamp(definition, lastTimestamp, bulkProcessor);
                }

            } catch (InterruptedException e) {
                logger.info("river-mongodb indexer interrupted");
                this.bulkProcessor.close();
                Thread.currentThread().interrupt();
                break;
            }
            logStatistics();
        }
    }

    @SuppressWarnings({ "unchecked" })
    private BSONTimestamp processBlockingQueue(QueueEntry entry) {
        if (entry.getData().get(MongoDBRiver.MONGODB_ID_FIELD) == null
                && !entry.getOperation().equals(MongoDBRiver.OPLOG_COMMAND_OPERATION)) {
            logger.warn("Cannot get object id. Skip the current item: [{}]", entry.getData());
            return null;
        }

        BSONTimestamp lastTimestamp = entry.getOplogTimestamp();
        String operation = entry.getOperation();
        if (MongoDBRiver.OPLOG_COMMAND_OPERATION.equals(operation)) {
            try {
                updateBulkRequest(entry.getData(), null, operation, definition.getIndexName(), definition.getTypeName(), null, null);
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
                updateBulkRequest(entry.getData(), objectId, operation, definition.getIndexName(), definition.getTypeName(), null, null);
            } catch (IOException ioEx) {
                logger.error("Update bulk failed.", ioEx);
            }
            return lastTimestamp;
        }

        if (hasScript() && definition.isAdvancedTransformation()) {
            return applyAdvancedTransformation(entry);
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
        if (hasScript()) {
            if (ctx != null) {
                ctx.put("document", entry.getData());
                ctx.put("operation", operation);
                if (!objectId.isEmpty()) {
                    ctx.put("id", objectId);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Script to be executed: {} - {}", definition.getScriptType(), definition.getScript());
                    logger.debug("Context before script executed: {}", ctx);
                }
                try {
                    ExecutableScript executableScript = scriptService.executable(definition.getScriptType(), definition.getScript(),
                            ImmutableMap.of("logger", logger));
                    executableScript.setNextVar("ctx", ctx);
                    executableScript.run();
                    // we need to unwrap the context object...
                    ctx = (Map<String, Object>) executableScript.unwrap(ctx);
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
            updateBulkRequest(new BasicDBObject(data), objectId, operation, index, type, routing, parent);
        } catch (IOException e) {
            logger.warn("failed to parse {}", e, entry.getData());
        }
        return lastTimestamp;
    }

    private void updateBulkRequest(DBObject data, String objectId, String operation, String index, String type, String routing,
            String parent) throws IOException {
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
            bulkProcessor.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));
            insertedDocuments.incrementAndGet();
        }
        if (MongoDBRiver.OPLOG_UPDATE_OPERATION.equals(operation)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Update operation - id: {} - contains attachment: {}", objectId, isAttachment);
            }
            deleteBulkRequest(objectId, index, type, routing, parent);
            bulkProcessor.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));
            updatedDocuments.incrementAndGet();
        }
        if (MongoDBRiver.OPLOG_DELETE_OPERATION.equals(operation)) {
            logger.info("Delete request [{}], [{}], [{}]", index, type, objectId);
            deleteBulkRequest(objectId, index, type, routing, parent);
            deletedDocuments.incrementAndGet();
        }
        if (MongoDBRiver.OPLOG_COMMAND_OPERATION.equals(operation)) {
            if (definition.isDropCollection()) {
                if ((data.get(MongoDBRiver.OPLOG_DROP_COMMAND_OPERATION) != null
                        && data.get(MongoDBRiver.OPLOG_DROP_COMMAND_OPERATION).equals(definition.getMongoCollection()) || (data
                        .get(MongoDBRiver.OPLOG_DROP_DATABASE_COMMAND_OPERATION) != null && data.get(
                        MongoDBRiver.OPLOG_DROP_DATABASE_COMMAND_OPERATION).equals(1)))) {
                    logger.info("Drop collection request [{}], [{}]", index, type);

                    // Delete / create type is now done in beforeBulk
                    bulkProcessor.add(indexRequest(index).type(type).source(DROP_COLLECTION));
                    flushBulkProcessor = true;
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
    private void deleteBulkRequest(String objectId, String index, String type, String routing, String parent) {
        logger.trace("bulkDeleteRequest - objectId: {} - index: {} - type: {} - routing: {} - parent: {}", objectId, index, type, routing,
                parent);

        if (definition.getParentTypes() != null && definition.getParentTypes().contains(type)) {
            QueryBuilder builder = QueryBuilders.hasParentQuery(type, QueryBuilders.termQuery(MongoDBRiver.MONGODB_ID_FIELD, objectId));
            SearchResponse response = client.prepareSearch(index).setQuery(builder).setRouting(routing)
                    .addField(MongoDBRiver.MONGODB_ID_FIELD).execute().actionGet();
            for (SearchHit hit : response.getHits().getHits()) {
                bulkProcessor.add(deleteRequest(index).type(hit.getType()).id(hit.getId()).routing(routing).parent(objectId));
            }
        }
        bulkProcessor.add(deleteRequest(index).type(type).id(objectId).routing(routing).parent(parent));
    }

    @SuppressWarnings("unchecked")
    private BSONTimestamp applyAdvancedTransformation(QueueEntry entry) {

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

        if (hasScript()) {
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
                try {
                    ExecutableScript executableScript = scriptService.executable(definition.getScriptType(), definition.getScript(),
                            ImmutableMap.of("logger", logger));
                    if (logger.isDebugEnabled()) {
                        logger.debug("Script to be executed: {} - {}", definition.getScriptType(), definition.getScript());
                        logger.debug("Context before script executed: {}", ctx);
                    }
                    executableScript.setNextVar("ctx", ctx);
                    executableScript.run();
                    // we need to unwrap the context object...
                    ctx = (Map<String, Object>) executableScript.unwrap(ctx);
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
                                updateBulkRequest(new BasicDBObject(data), objectId, operation, index, type, routing, parent);
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

    private boolean hasScript() {
        return definition.getScriptType() != null && definition.getScript() != null;
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
        long totalDocuments = deletedDocuments.get() + insertedDocuments.get();
        long totalTimeInSeconds = sw.stop().totalTime().seconds();
        long totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? totalDocuments : totalDocuments / totalTimeInSeconds;
        logger.info("Indexed {} documents, {} insertions, {} updates, {} deletions, {} documents per second", totalDocuments,
                insertedDocuments.get(), updatedDocuments.get(), deletedDocuments.get(), totalDocumentsPerSecond);
    }

    private void dropRecreateMapping(String index, String type) throws IOException {
        client.admin().indices().prepareRefresh(index).execute().actionGet();
        Map<String, MappingMetaData> mappings = client.admin().cluster().prepareState().execute().actionGet().getState().getMetaData()
                .index(index).mappings();
        logger.trace("mappings contains type {}: {}", type, mappings.containsKey(type));
        if (mappings.containsKey(type)) {
            /*
             * Issue #105 - Mapping changing from custom mapping to dynamic when
             * drop_collection = true Should capture the existing mapping
             * metadata (in case it is has been customized before to delete.
             */
            MappingMetaData mapping = mappings.get(type);
            client.admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet();
            PutMappingResponse pmr = client.admin().indices().preparePutMapping(index).setType(type).setSource(mapping.getSourceAsMap())
                    .execute().actionGet();
            if (!pmr.isAcknowledged()) {
                logger.error("Failed to put mapping {} / {} / {}.", index, type, mapping.source());
            } else {
                logger.info("Delete and recreate for index / type [{}] [{}] successfully executed.", index, type);
            }
        }
    }
}