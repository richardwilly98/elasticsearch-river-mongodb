package org.elasticsearch.river.mongodb;

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class MongoDBRiverBulkProcessor {

    public static final Map<String, Boolean> DROP_INDEX = ImmutableMap.of("dropIndex", Boolean.TRUE);
    private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());
    private final MongoDBRiverDefinition definition;
    private final SharedContext context;
    private final Client client;
    private final BulkProcessor bulkProcessor;
    private final String index;
    private final String type;

    private final AtomicBoolean flushBulkProcessor = new AtomicBoolean();
    private final AtomicInteger deletedDocuments = new AtomicInteger();
    private final AtomicInteger insertedDocuments = new AtomicInteger();
    private final AtomicInteger updatedDocuments = new AtomicInteger();
    private final AtomicLong documentCount = new AtomicLong();
    private final static Semaphore semaphore = new Semaphore(1);

    public static class Builder {

        private final MongoDBRiverDefinition definition;
        private final SharedContext context;
        private final Client client;
        private String index;
        private String type;

        public Builder(MongoDBRiverDefinition definition, SharedContext context, Client client, String index, String type) {
            this.definition = definition;
            this.context = context;
            this.client = client;
            this.index = index;
            this.type = type;
        }

        public MongoDBRiverBulkProcessor build() {
            return new MongoDBRiverBulkProcessor(definition, context, client, index, type);
        }
    }

    private final BulkProcessor.Listener listener = new Listener() {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            logger.trace("beforeBulk - new bulk [{}] of items [{}]", executionId, request.numberOfActions());
            if (flushBulkProcessor.get()) {
                logger.info("About to flush bulk request index[{}] - type[{}]", index, type);
                int dropDollectionIndex = findLastDropCollection(request.requests());
                request.requests().subList(0, dropDollectionIndex + 1).clear();
                try {
                    dropRecreateMapping();
                    deletedDocuments.set(0);
                    updatedDocuments.set(0);
                    insertedDocuments.set(0);
                    flushBulkProcessor.set(false);
                } catch (Throwable t) {
                    logger.error("Drop collection operation failed", t);
                    context.setStatus(Status.IMPORT_FAILED);
                }
            }
        }

        @SuppressWarnings("rawtypes")
        private int findLastDropCollection(List<ActionRequest> request) {
            int index = 0;
            for (int i = 0; i < request.size(); i++) {
                ActionRequest<?> action = request.get(i);
                if (action instanceof IndexRequest) {
                    Map<String, Object> source = ((IndexRequest) action).sourceAsMap();
                    if (source.equals(DROP_INDEX)) {
                        index = i;
                    }
                }
            }
            return index;
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
                logStatistics();
                if (logger.isTraceEnabled()) {
                    logger.trace("afterBulk - bulk [{}] success [{} items] [{} ms] total [{}]", executionId, response.getItems().length,
                            response.getTookInMillis(), documentCount.get());
                }
            }
        }
    };

    MongoDBRiverBulkProcessor(MongoDBRiverDefinition definition, SharedContext context, Client client, String index, String type) {
        this.bulkProcessor = BulkProcessor.builder(client, listener).setBulkActions(definition.getBulk().getBulkActions())
                .setConcurrentRequests(definition.getBulk().getConcurrentRequests())
                .setFlushInterval(definition.getBulk().getFlushInterval()).setBulkSize(definition.getBulk().getBulkSize()).build();
        this.definition = definition;
        this.context = context;
        this.client = client;
        this.index = index;
        this.type = type;
    }

    public void dropIndex() {
        addBulkRequest(null, DROP_INDEX, null, null);
        flushBulkProcessor.set(true);
    }

    public void addBulkRequest(String id, Map<?, ?> source, String routing, String parent) {
        bulkProcessor.add(indexRequest(index).type(type).id(id).source(source).routing(routing).parent(parent));
        insertedDocuments.incrementAndGet();
    }

    public void addBulkRequest(String id, XContentBuilder source, String routing, String parent) {
        bulkProcessor.add(indexRequest(index).type(type).id(id).source(source).routing(routing).parent(parent));
        insertedDocuments.incrementAndGet();
    }

//    public void updateBulkRequest(String id, XContentBuilder source, String routing, String parent) {
//        deleteBulkRequest(id, routing, parent);
//        bulkProcessor.add(indexRequest(index).type(type).id(id).source(source).routing(routing).parent(parent));
//        updatedDocuments.incrementAndGet();
//    }

    public void deleteBulkRequest(String id, String routing, String parent) {
        logger.trace("deleteBulkRequest - id: {} - index: {} - type: {} - routing: {} - parent: {}", id, index, type, routing, parent);
        bulkProcessor.add(deleteRequest(index).type(type).id(id).routing(routing).parent(parent));
        deletedDocuments.incrementAndGet();
    }

    public BulkProcessor getBulkProcessor() {
        return bulkProcessor;
    }

    private void dropRecreateMapping() throws IOException, InterruptedException {
        try {
            semaphore.acquire();
            logger.trace("dropRecreateMapping index[{}] - type[{}]", index, type);
            client.admin().indices().prepareRefresh(index).get();
            Map<String, MappingMetaData> mappings = client.admin().cluster().prepareState().get().getState().getMetaData().index(index)
                    .mappings();
            logger.trace("mappings contains type {}: {}", type, mappings.containsKey(type));
            if (mappings.containsKey(type)) {
                /*
                 * Issue #105 - Mapping changing from custom mapping to dynamic
                 * when drop_collection = true Should capture the existing
                 * mapping metadata (in case it is has been customized before to
                 * delete.
                 */
                MappingMetaData mapping = mappings.get(type);
                if (client.admin().indices().prepareDeleteMapping(index).setType(type).get().isAcknowledged()) {
                    PutMappingResponse pmr = client.admin().indices().preparePutMapping(index).setType(type)
                            .setSource(mapping.getSourceAsMap()).get();
                    if (!pmr.isAcknowledged()) {
                        logger.error("Failed to put mapping {} / {} / {}.", index, type, mapping.source());
                    } else {
                        logger.info("Delete and recreate for index / type [{}] [{}] successfully executed.", index, type);
                    }
                } else {
                    logger.warn("Delete type[{}] on index[{}] return aknowledge false", type, index);
                }
            } else {
                logger.info("type[{}] does not exist in index[{}]. No need to remove mapping.", index, type);
            }
        } finally {
            semaphore.release();
        }
    }

    // TODO: Still interested in timing / duration?
    private void logStatistics() {
        long totalDocuments = deletedDocuments.get() + insertedDocuments.get();
        logger.debug("Indexed {} documents, {} insertions, {} updates, {} deletions", totalDocuments, insertedDocuments.get(),
                updatedDocuments.get(), deletedDocuments.get());
        if (definition.isStoreStatistics()) {
            Map<String, Object> source = new HashMap<String, Object>();
            source.put("date", new Date());
            source.put("index", index);
            source.put("type", type);
            source.put("documents.inserted", insertedDocuments.get());
            source.put("documents.updated", updatedDocuments.get());
            source.put("documents.deleted", deletedDocuments.get());
            client.prepareIndex(definition.getRiverIndexName(), definition.getRiverName()).setSource(source).get();
        }
    }
}
