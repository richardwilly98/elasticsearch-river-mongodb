package org.elasticsearch.rest.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
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
import org.elasticsearch.river.RiversService;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.search.SearchHit;

public class RestMongoDBRiverAction extends BaseRestHandler {

	private final RiversService riversService;

	@Inject
	public RestMongoDBRiverAction(Settings settings, Client client,
			RestController controller, Injector injector) {
		super(settings, client);
		this.riversService = injector.getInstance(RiversService.class);
		controller.registerHandler(RestRequest.Method.GET, "/_river/"
				+ MongoDBRiver.TYPE + "/{action}", this);
		controller.registerHandler(RestRequest.Method.POST, "/_river/"
				+ MongoDBRiver.TYPE + "/{river}/{action}", this);
		controller.registerHandler(RestRequest.Method.POST, "/_river/"
				+ MongoDBRiver.TYPE + "/{river}/{action}", this);
	}

	@Override
	public void handleRequest(final RestRequest request, RestChannel channel) {
		// Get and check river name parameter
		String riverName = request.param("river");
		String uri = request.uri();
		logger.trace("uri: {}", uri);
		logger.trace("action: {}", request.param("action"));
		String action = null;
		if (uri.endsWith("_start")) {
			action = "start";
		} else if (uri.endsWith("_stop")) {
			action = "stop";
		} else if (uri.endsWith("_list")) {
			action = "list";
		}

		if ("start".equals(action) || "stop".equals(action)) {
			if (riverName == null || riverName.isEmpty()) {
				respond(false, request, channel,
						"Parameter 'river' is required", RestStatus.BAD_REQUEST);
				return;
			}
		}

		logger.warn("Start river: {} - action: {}", riverName, action);

		if ("list".equals(action)) {
			List<Map<String, Object>> rivers = getRivers();
			try {
				XContentBuilder builder = RestXContentBuilder
						.restContentBuilder(request);
				// builder.startObject();
				builder.value(rivers);
				// builder.endObject();
				channel.sendResponse(new XContentRestResponse(request,
						RestStatus.OK, builder));
			} catch (IOException e) {
				errorResponse(request, channel, e);
			}

		} else {
//			String status = "started";
			boolean enabled = true;
			if ("stop".equals(action)) {
//				status = "stopped";
				enabled = false;
			}
			boolean success = true;
//			try {
//				setRiverStatus(riverName, status);
				MongoDBRiverHelper.setRiverEnabled(client, riverName, enabled);
//			} catch (IOException ioEx) {
//				success = false;
//			}
			String error = !success ? null : "River not found: " + riverName;
			respond(success, request, channel, error, RestStatus.OK);
		}
	}

	private void respond(boolean success, RestRequest request,
			RestChannel channel, String error, RestStatus status) {
		try {
			XContentBuilder builder = RestXContentBuilder
					.restContentBuilder(request);
			builder.startObject();
			builder.field("success", success);
			if (error != null) {
				builder.field("error", error);
			}
			builder.endObject();
			channel.sendResponse(new XContentRestResponse(request, status,
					builder));
		} catch (IOException e) {
			errorResponse(request, channel, e);
		}
	}

	private void errorResponse(RestRequest request, RestChannel channel,
			Throwable e) {
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

//	private void setRiverStatus(String riverName, String status)
//			throws IOException {
//		XContentBuilder xb = jsonBuilder().startObject().startObject("mongodb")
//				.field("status", status).endObject().endObject();
//		client.prepareIndex("_river", riverName, "_mongodbstatus")
//				.setSource(xb).execute().actionGet();
//	}
}
