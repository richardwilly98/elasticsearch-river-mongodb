package org.elasticsearch.river.mongodb;

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.threadpool.ThreadPool.Info;
import org.elasticsearch.threadpool.ThreadPoolStats.Stats;

public class MongoDBRiverBulkProcessor extends MongoDBRiverComponent {

    public static final long DEFAULT_BULK_QUEUE_SIZE = 50;
    public static final Map<String, Boolean> DROP_INDEX = ImmutableMap.of("dropIndex", Boolean.TRUE);
    private final MongoDBRiver river;
    private final MongoDBRiverDefinition definition;
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

    private final long bulkQueueSize;
    
    public static class Builder {

        private final MongoDBRiver river;
        private final MongoDBRiverDefinition definition;
        private final Client client;
        private String index;
        private String type;

        public Builder(MongoDBRiver river, MongoDBRiverDefinition definition, Client client, String index, String type) {
            this.river = river;
            this.definition = definition;
            this.client = client;
            this.index = index;
            this.type = type;
        }

        public MongoDBRiverBulkProcessor build() {
            return new MongoDBRiverBulkProcessor(river, definition, client, index, type);
        }
    }

    private final BulkProcessor.Listener listener = new Listener() {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            checkBulkProcessorAvailability();
            logger.trace("beforeBulk - new bulk [{}] of items [{}]", executionId, request.numberOfActions());
            if (flushBulkProcessor.get()) {
                logger.trace("About to flush bulk request index[{}] - type[{}]", index, type);
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
                    MongoDBRiverHelper.setRiverStatus(client, definition.getRiverName(), Status.IMPORT_FAILED);
                    request.requests().clear();
                    bulkProcessor.close();
                    river.close();
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
                MongoDBRiverHelper.setRiverStatus(client, definition.getRiverName(), Status.IMPORT_FAILED);
                request.requests().clear();
                bulkProcessor.close();
                river.close();
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                logger.error("Bulk processor failed. {}", response.buildFailureMessage());
                MongoDBRiverHelper.setRiverStatus(client, definition.getRiverName(), Status.IMPORT_FAILED);
                request.requests().clear();
                bulkProcessor.close();
                river.close();
            } else {
                documentCount.addAndGet(response.getItems().length);
                logStatistics(response.getTookInMillis());
                deletedDocuments.set(0);
                updatedDocuments.set(0);
                insertedDocuments.set(0);
                if (logger.isTraceEnabled()) {
                    logger.trace("afterBulk - bulk [{}] success [{} items] [{} ms] total [{}]", executionId, response.getItems().length,
                            response.getTookInMillis(), documentCount.get());
                }
            }
        }
    };

    MongoDBRiverBulkProcessor(MongoDBRiver river, MongoDBRiverDefinition definition, Client client, String index, String type) {
        super(river);
        this.river = river;
        this.bulkProcessor = BulkProcessor.builder(client, listener).setBulkActions(definition.getBulk().getBulkActions())
                .setConcurrentRequests(definition.getBulk().getConcurrentRequests())
                .setFlushInterval(definition.getBulk().getFlushInterval()).setBulkSize(definition.getBulk().getBulkSize()).build();
        this.definition = definition;
        this.client = client;
        this.index = index;
        this.type = type;
        this.bulkQueueSize = getBulkQueueSize();
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

    // public void updateBulkRequest(String id, XContentBuilder source, String
    // routing, String parent) {
    // deleteBulkRequest(id, routing, parent);
    // bulkProcessor.add(indexRequest(index).type(type).id(id).source(source).routing(routing).parent(parent));
    // updatedDocuments.incrementAndGet();
    // }

    public void deleteBulkRequest(String id, String routing, String parent) {
        logger.trace("deleteBulkRequest - id: {} - index: {} - type: {} - routing: {} - parent: {}", id, index, type, routing, parent);
        bulkProcessor.add(deleteRequest(index).type(type).id(id).routing(routing).parent(parent));
        deletedDocuments.incrementAndGet();
    }

    public BulkProcessor getBulkProcessor() {
        return bulkProcessor;
    }

    private void checkBulkProcessorAvailability() {
        while (!isBulkProcessorAvailable()) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Waiting for bulk queue to empty...");
                }
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                logger.warn("checkIndexStatistics interrupted", e);
            }
        }
    }

    private long getBulkQueueSize() {
        NodesInfoResponse response = client.admin().cluster().prepareNodesInfo().setThreadPool(true).get();
        for (NodeInfo node : response.getNodes()) {
            Iterator<Info> iterator = node.getThreadPool().iterator();
            while (iterator.hasNext()) {
                Info info = iterator.next();
                if ("bulk".equals(info.getName())) {
                    return info.getQueueSize().getSingles();
                }
            }
        }
        return DEFAULT_BULK_QUEUE_SIZE;
    }
    private boolean isBulkProcessorAvailable() {
        NodesStatsResponse response = client.admin().cluster().prepareNodesStats().setThreadPool(true).get();
        for (NodeStats nodeStats : response.getNodes()) {
            Iterator<Stats> iterator = nodeStats.getThreadPool().iterator();
            while (iterator.hasNext()) {
                Stats stats = iterator.next();
                if ("bulk".equals(stats.getName())) {
                    int queue = stats.getQueue();
                    logger.trace("bulkQueueSize [{}] - queue [{}] - availability [{}]", bulkQueueSize, queue, 1 - (queue / bulkQueueSize));
                    return 1 - (queue / bulkQueueSize) > 0.1;
                }
            }
        }
        return true;
    }

    private void dropRecreateMapping() throws IOException, InterruptedException {
        try {
            semaphore.acquire();
            logger.trace("dropRecreateMapping index[{}] - type[{}]", index, type);
            client.admin().indices().prepareRefresh(index).get();
            ImmutableOpenMap<String, MappingMetaData> mappings = client.admin().cluster().prepareState().get().getState().getMetaData()
                    .index(index).mappings();
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

    private void logStatistics(long duration) {
        if (definition.isStoreStatistics()) {
            long totalDocuments = deletedDocuments.get() + insertedDocuments.get();
            logger.trace("Indexed {} documents: {} insertions, {} updates, {} deletions", totalDocuments, insertedDocuments.get(),
                    updatedDocuments.get(), deletedDocuments.get());
            Map<String, Object> source = new HashMap<String, Object>();
            Map<String, Object> statistics = Maps.newHashMap();
            statistics.put("duration", duration);
            statistics.put("date", new Date());
            statistics.put("index", index);
            statistics.put("type", type);
            statistics.put("documents.inserted", insertedDocuments.get());
            statistics.put("documents.updated", updatedDocuments.get());
            statistics.put("documents.deleted", deletedDocuments.get());
            statistics.put("documents.total", documentCount.get());
            source.put("statistics", statistics);
            client.prepareIndex(definition.getStatisticsIndexName(), definition.getStatisticsTypeName()).setSource(source).get();
        }
    }
}
