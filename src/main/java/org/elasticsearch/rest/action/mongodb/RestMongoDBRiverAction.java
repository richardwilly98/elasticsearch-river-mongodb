package org.elasticsearch.rest.action.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.river.mongodb.Timestamp;
import org.elasticsearch.river.mongodb.rest.XContentThrowableRestResponse;
import org.elasticsearch.river.mongodb.rest.action.support.RestXContentBuilder;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.search.SearchHit;

public class RestMongoDBRiverAction extends BaseRestHandler {

    private final String riverIndexName;

    @Inject
    public RestMongoDBRiverAction(Settings settings, Client client, RestController controller, @RiverIndexName String riverIndexName) {
        super(settings, client);
        this.riverIndexName = riverIndexName;
        String baseUrl = "/" + riverIndexName + "/" + MongoDBRiver.TYPE;
        logger.trace("RestMongoDBRiverAction - baseUrl: {}", baseUrl);
        controller.registerHandler(RestRequest.Method.GET, baseUrl + "/{action}", this);
        controller.registerHandler(RestRequest.Method.POST, baseUrl + "/{river}/{action}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        logger.debug("uri: {}", request.uri());
        logger.debug("action: {}", request.param("action"));

        if (request.path().endsWith("list")) {
            list(request, channel, client);
            return;
        } else if (request.path().endsWith("start")) {
            start(request, channel, client);
            return;
        } else if (request.path().endsWith("stop")) {
            stop(request, channel, client);
            return;
        } else if (request.path().endsWith("delete")) {
            delete(request, channel, client);
            return;
        }

        respondError(request, channel, "action not found: " + request.uri(), RestStatus.OK);
    }

    private void delete(RestRequest request, RestChannel channel, Client client) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        logger.info("Delete river: {}", river);
        if (client.admin().indices().prepareTypesExists(riverIndexName).setTypes(river).get().isExists()) {
            client.admin().indices().prepareDeleteMapping(riverIndexName).setType(river).get();
        }
        respondSuccess(request, channel, RestStatus.OK);
    }

    private void start(RestRequest request, RestChannel channel, Client client) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        MongoDBRiverHelper.setRiverStatus(client, river, Status.RUNNING);
        respondSuccess(request, channel, RestStatus.OK);
    }

    private void stop(RestRequest request, RestChannel channel, Client client) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        MongoDBRiverHelper.setRiverStatus(client, river, Status.STOPPED);
        respondSuccess(request, channel, RestStatus.OK);
    }

    private void list(RestRequest request, RestChannel channel, Client client) {
        try {
            Map<String, Object> rivers = getRivers(request.paramAsInt("page", 1), request.paramAsInt("count", 10), client);
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.value(rivers);
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        } catch (Throwable e) {
            errorResponse(request, channel, e);
        }
    }

    private void respondSuccess(RestRequest request, RestChannel channel, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.field("success", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    private void respondError(RestRequest request, RestChannel channel, String error, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.field("success", false);
            builder.field("error", error);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    private void errorResponse(RestRequest request, RestChannel channel, Throwable e) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, e));
            logger.error("errorResponse", e);
        } catch (IOException ioEx) {
            logger.error("Failed to send failure response", ioEx);
        }
    }

    private Map<String, Object> getRivers(int page, int count, Client client) {
        int from = (page - 1) * count;
        SearchResponse searchResponse = client.prepareSearch(riverIndexName)
                .setQuery(QueryBuilders.queryString(MongoDBRiver.TYPE).defaultField("type")).setFrom(from).setSize(count).get();
        long totalHits = searchResponse.getHits().totalHits();
        logger.trace("totalHits: {}", totalHits);
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("hits", totalHits);
        data.put("page", page);
        data.put("pages", Math.ceil(totalHits / count) + 1);
        List<Map<String, Object>> rivers = new ArrayList<Map<String, Object>>();
        int i = 0;
        for (SearchHit hit : searchResponse.getHits().hits()) {
            Map<String, Object> source = new HashMap<String, Object>();
            String riverName = hit.getType();
            RiverSettings riverSettings = new RiverSettings(null, hit.getSource());
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName, riverIndexName, riverSettings, null);

            Timestamp<?> ts = MongoDBRiver.getLastTimestamp(client, definition);
            Long lastTimestamp = null;
            if (ts != null) {
                lastTimestamp = ts.getTime();
            }
            source.put("name", riverName);
            source.put("status", MongoDBRiverHelper.getRiverStatus(client, riverName));
            source.put("settings", hit.getSource());
            source.put("lastTimestamp", lastTimestamp);
            source.put("indexCount", MongoDBRiver.getIndexCount(client, definition));
            if (logger.isTraceEnabled()) {
                logger.trace("source: {}", hit.getSourceAsString());
            }
            rivers.add(source);
            i++;
        }
        data.put("count", i);
        data.put("results", rivers);
        return data;
    }

}
