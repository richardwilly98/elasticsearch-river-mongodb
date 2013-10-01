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
import org.elasticsearch.river.mongodb.Status;

public abstract class MongoDBRiverHelper {

    private static final ESLogger logger = Loggers.getLogger(MongoDBRiverHelper.class);

    public static Status getRiverStatus(Client client, String riverName) {
        GetResponse statusResponse = client.prepareGet("_river", riverName, MongoDBRiver.STATUS_ID).execute().actionGet();
        if (!statusResponse.isExists()) {
            setRiverStatus(client, riverName, Status.RUNNING);
            return Status.RUNNING;
        } else {
            Object obj = XContentMapValues.extractValue(MongoDBRiver.TYPE + "." + MongoDBRiver.STATUS_FIELD, statusResponse.getSourceAsMap());
            return Status.valueOf(obj.toString());
        }
    }

    public static void setRiverStatus(Client client, String riverName, Status status) {
        logger.debug("setRiverStatus called with {}", status);
        XContentBuilder xb;
        try {
            xb = jsonBuilder().startObject().startObject(MongoDBRiver.TYPE).field(MongoDBRiver.STATUS_FIELD, status).endObject().endObject();
            client.prepareIndex("_river", riverName, MongoDBRiver.STATUS_ID).setSource(xb).execute().actionGet();
        } catch (IOException ioEx) {
            logger.error("setRiverStatus failed for river {}", ioEx, riverName);
        }
    }

}
