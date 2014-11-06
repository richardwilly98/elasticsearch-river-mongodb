package org.elasticsearch.river.mongodb.tokumx;

import static org.elasticsearch.client.Requests.countRequest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.hamcrest.Matchers;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

@Test
public class RiverTokuMXOplogRefTest extends RiverTokuMXTestAbstract {

    static final String LONG_STRING =
            "abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz";

    private DB mongoDB;
    private DBCollection mongoCollection;
    
    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);
            logger.info("Start createCollection");
            this.mongoCollection = mongoDB.createCollection(getCollection(), null);
            Assert.assertNotNull(mongoCollection);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    @AfterClass
    public void cleanUp() {
        super.deleteRiver();
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @Test
    public void testOplogRefs() throws InterruptedException {
        mongoCollection.insert(buildLargeObject(), WriteConcern.REPLICAS_SAFE);
        Thread.sleep(wait);
        ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                .exists(new IndicesExistsRequest(getIndex()));
        assertThat(response.actionGet().isExists(), equalTo(true));
        refreshIndex();
        CountResponse countResponse = getNode().client().count(countRequest(getIndex())).actionGet();
        assertThat(countResponse.getCount(), Matchers.equalTo(1L));
        try (DBCursor cursor = mongoDB.getSisterDB(LOCAL_DATABASE_NAME).getCollection(OPLOG_COLLECTION)
                .find().sort(new BasicDBObject("$natural", -1)).limit(1)) {
            DBObject lastOplog = cursor.toArray().get(0);
            assertThat(lastOplog.containsField("ref"), Matchers.is(Boolean.TRUE));
        }
    }

    private static BasicDBObject buildLargeObject() {
        BasicDBObject core = new BasicDBObject();
        for (char c = 'a'; c <= 'z'; ++c) {
            core.append("" + c, LONG_STRING);
        }
        List<DBObject> list1 = new ArrayList<DBObject>(10);
        for (int k = 1; k <= 10; ++k) {
            list1.add(new BasicDBObject("k", k).append("v", core));
        }
        List<DBObject> list2 = new ArrayList<DBObject>(10);
        for (int j = 1; j <= 10; ++j) {
            list2.add(new BasicDBObject("j", j).append("v", list1));
        }
        List<DBObject> list3 = new ArrayList<DBObject>(10);
        for (int i = 1; i <= 10; ++i) {
            list3.add(new BasicDBObject("i", i).append("v", list2));
        }
        return new BasicDBObject("_id", 0).append("o", list3);
    }
}
