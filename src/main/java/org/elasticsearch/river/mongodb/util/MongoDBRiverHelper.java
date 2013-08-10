package org.elasticsearch.river.mongodb.util;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.mongodb.MongoDBRiver;

public abstract class MongoDBRiverHelper {

	private static final ESLogger logger = Loggers
			.getLogger(MongoDBRiverHelper.class);

	public static boolean isRiverEnabled(Client client, String riverName) {
		boolean enabled = true;
		GetResponse getResponse = client
				.prepareGet("_river", riverName, MongoDBRiver.STATUS).execute()
				.actionGet();

		if (!getResponse.isExists()) {
			setRiverEnabled(client, riverName, true);
		} else {
			Object obj = XContentMapValues.extractValue(MongoDBRiver.TYPE + "."
					+ MongoDBRiver.ENABLED, getResponse.getSourceAsMap());
			if (obj != null) {
				enabled = Boolean.parseBoolean(obj.toString());
			}
		}
//		logger.trace("River {} enabled? {}", riverName, enabled);
		return enabled;
	}

	public static void setRiverEnabled(Client client, String riverName, boolean enabled) {
		XContentBuilder xb;
		try {
			xb = jsonBuilder().startObject().startObject(MongoDBRiver.TYPE)
					.field(MongoDBRiver.ENABLED, enabled).endObject()
					.endObject();
			client.prepareIndex("_river", riverName, MongoDBRiver.STATUS)
					.setSource(xb).execute().actionGet();
		} catch (IOException ioEx) {
			logger.error("setRiverEnabled failed for river {}", ioEx, riverName);
		}
	}
}
