package org.elasticsearch.rest.action;

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
import org.elasticsearch.index.query.FieldQueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.search.SearchHit;

public class RestMongoDBRiverAction extends BaseRestHandler {

    private final String riverIndexName;

    @Inject
    public RestMongoDBRiverAction(Settings settings, Client client, RestController controller, @RiverIndexName String riverIndexName) {
        super(settings, client);
        this.riverIndexName = riverIndexName;
        String baseUrl = "/" + riverIndexName + "/" + MongoDBRiver.TYPE;
        controller.registerHandler(RestRequest.Method.GET, baseUrl + "/{action}", this);
        controller.registerHandler(RestRequest.Method.POST, baseUrl + "/{river}/{action}", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        String uri = request.uri();
        logger.trace("uri: {}", uri);
        logger.trace("action: {}", request.param("action"));

        if (uri.endsWith("list")) {
            list(request, channel);
            return;
        } else if (uri.endsWith("start")) {
            start(request, channel);
            return;
        } else if (uri.endsWith("stop")) {
            stop(request, channel);
            return;
        } else if (uri.endsWith("delete")) {
            delete(request, channel);
            return;
        }

        respondError(request, channel, "action not found: " + uri, RestStatus.OK);
    }

    private void delete(RestRequest request, RestChannel channel) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        logger.info("Delete river: {}", river);
        if (client.admin().indices().prepareTypesExists(riverIndexName).setTypes(river).get().isExists()) {
            client.admin().indices().prepareDeleteMapping(riverIndexName).setType(river).execute().actionGet();
        }
        respondSuccess(request, channel, RestStatus.OK);
    }

    private void start(RestRequest request, RestChannel channel) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        MongoDBRiverHelper.setRiverStatus(client, river, Status.RUNNING);
        respondSuccess(request, channel, RestStatus.OK);
    }

    private void stop(RestRequest request, RestChannel channel) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        MongoDBRiverHelper.setRiverStatus(client, river, Status.STOPPED);
        respondSuccess(request, channel, RestStatus.OK);
    }

    private void list(RestRequest request, RestChannel channel) {
        List<Map<String, Object>> rivers = getRivers();
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.value(rivers);
            channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    private void respondSuccess(RestRequest request, RestChannel channel, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.field("success", true);
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, status, builder));
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
            channel.sendResponse(new XContentRestResponse(request, status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    private void errorResponse(RestRequest request, RestChannel channel, Throwable e) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, e));
        } catch (IOException ioEx) {
            logger.error("Failed to send failure response", ioEx);
        }
    }

    private List<Map<String, Object>> getRivers() {
        SearchResponse searchResponse = client.prepareSearch(riverIndexName)
                .setQuery(new FieldQueryBuilder("type", "mongodb"))
                .execute().actionGet();
        long totalHits = searchResponse.getHits().totalHits();
        logger.trace("totalHits: {}", totalHits);
        List<Map<String, Object>> rivers = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : searchResponse.getHits().hits()) {
            Map<String, Object> source = new HashMap<String, Object>();
            String riverName = hit.getType();
            RiverSettings riverSettings = new RiverSettings(null, hit.getSource());
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName, riverIndexName, riverSettings, null);

            source.put("name", riverName);
            source.put("status", MongoDBRiverHelper.getRiverStatus(client, riverName));
            source.put("settings", hit.getSource());
            source.put("lastTimestamp", 1000L * MongoDBRiver.getLastTimestamp(client, definition).getTime());
            source.put("indexCount", MongoDBRiver.getIndexCount(client, definition));
            logger.trace("source: {}", hit.getSourceAsString());
            rivers.add(source);
        }
        return rivers;
    }

}
