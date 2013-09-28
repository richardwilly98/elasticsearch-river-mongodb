package org.elasticsearch.rest.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FieldQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.search.SearchHit;

public class RestMongoDBRiverAction extends BaseRestHandler {

	private static final String BASE_URL = "/_river/" + MongoDBRiver.TYPE;

	@Inject
	public RestMongoDBRiverAction(Settings settings, Client client, RestController controller) {
		super(settings, client);
		controller.registerHandler(RestRequest.Method.GET, BASE_URL + "/{action}", this);
		controller.registerHandler(RestRequest.Method.POST, BASE_URL + "/{river}/{action}", this);
		controller.registerHandler(RestRequest.Method.POST, BASE_URL + "/{river}/{action}", this);
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
		}

		respondError(request, channel, "action not found: " + uri, RestStatus.OK);
	}

	private void start(RestRequest request, RestChannel channel) {
		String river = request.param("river");
		if (river == null || river.isEmpty()) {
			respondError(request, channel,
					"Parameter 'river' is required", RestStatus.BAD_REQUEST);
			return;
		}
		MongoDBRiverHelper.setRiverEnabled(client, river, true);
		respondSuccess(request, channel, RestStatus.OK);		
	}

	private void stop(RestRequest request, RestChannel channel) {
		String river = request.param("river");
		if (river == null || river.isEmpty()) {
			respondError(request, channel,
					"Parameter 'river' is required", RestStatus.BAD_REQUEST);
			return;
		}
		MongoDBRiverHelper.setRiverEnabled(client, river, false);
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
	
	private void respondError(RestRequest request, RestChannel channel,
			String error, RestStatus status) {
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
		QueryBuilder query = new FieldQueryBuilder("type", "mongodb");
		SearchResponse searchResponse = client.prepareSearch("_river")
				.setQuery(query).execute().actionGet();
		long totalHits = searchResponse.getHits().totalHits();
		logger.trace("totalHits: {}", totalHits);
		List<Map<String, Object>> rivers = new ArrayList<Map<String, Object>>();
		for (SearchHit hit : searchResponse.getHits().hits()) {
			Map<String, Object> source = hit.getSource();
			source.put("_name", hit.getType());
			source.put("_enabled", MongoDBRiverHelper.isRiverEnabled(client, hit.getType()));
			logger.trace("source: {}", hit.getSourceAsString());
			rivers.add(source);
		}
		return rivers;
	}

}
