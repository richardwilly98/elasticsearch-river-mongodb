package org.elasticsearch.river.mongodb.simple;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.Assert;
import org.testng.annotations.*;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

import java.io.IOException;

public class RiverMongoDbRefTest extends RiverMongoDBTestAbstract {

    private static final String TEST_DBREF_MONGODB_DOCUMENT_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-document-with-dbref.json";
    private static final String TEST_MONGO_REFERENCED_DOCUMENT = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-referenced-document.json";

    private DB mongoDB;
    private DBCollection mongoCollection, referencedCollection;

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoDbRefTest(ExecutableType type) {
        super(type);
    }

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            logger.info("Start createCollection");
            this.mongoCollection = mongoDB.createCollection(getCollection(), new BasicDBObject());
            Assert.assertNotNull(mongoCollection);
            Assert.assertNotNull(referencedCollection);
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
    public void simpleBSONObject() throws Throwable {
        logger.debug("Start simpleBSONObject");
        super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);
        try {
            DBObject dbObject = setUp();
            String id = dbObject.get("_id").toString();
            String categoryId = ((DBRef) dbObject.get("category")).getId().toString();
            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(getIndex()));
            assertThat(response.actionGet().isExists(), equalTo(true));
            refreshIndex();
            SearchRequest search = getNode().client().prepareSearch().setIndices(getIndex())
                    .setQuery(QueryBuilders.queryString(categoryId).defaultField("category.id")).request();
            SearchResponse searchResponse = getNode().client().search(search).actionGet();
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
            assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(id));

            search = getNode().client().prepareSearch(getIndex()).setQuery(new QueryStringQueryBuilder("testing").defaultField("innerDoc.innerThing"))
                    .request();
            searchResponse = getNode().client().search(search).actionGet();
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

            // search =
            // getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.geoShapeQuery("location",
            // new GeoCircle(new Point, 20.0)))
            // .request();
            // searchResponse = getNode().client().search(search).actionGet();
            // assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

        } catch (Throwable t) {
            logger.error("simpleBSONObject failed.", t);
            t.printStackTrace();
            throw t;
        } finally {
            tearDown();
        }
    }

    @Test
    public void nestedDbRef() throws Throwable{
        super.createRiver(TEST_MONGODB_RIVER_EXPAND_DB_REFS_JSON);
        try {
            DBObject dbObject = setUp();
            ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices()
                    .exists(new IndicesExistsRequest(getIndex()));
            assertThat(response.actionGet().isExists(), equalTo(true));
            refreshIndex();

            SearchRequest search = getNode().client().prepareSearch(getIndex()).setQuery(new QueryStringQueryBuilder("arbitrary").defaultField("category.name")).addField("category.name")
                    .request();
            SearchResponse searchResponse = getNode().client().search(search).actionGet();
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
            Assert.assertEquals("arbitrary category for a thousand", searchResponse.getHits().getAt(0).field("category.name").getValue().toString());

        } catch (Throwable t) {
            logger.error("nestedDbRef failed.", t);
            t.printStackTrace();
            throw t;
        } finally{
            tearDown();
        }
    }

    DBObject setUp() throws IOException, InterruptedException {
        this.referencedCollection = mongoDB.createCollection("category", null);
        String referencedDocument = copyToStringFromClasspath(TEST_MONGO_REFERENCED_DOCUMENT);
        WriteResult result = referencedCollection.insert((DBObject) JSON.parse(referencedDocument));
        Thread.sleep(wait);
        logger.info("Referenced WriteResult: {}", result.toString());

        String mongoDocument = copyToStringFromClasspath(TEST_DBREF_MONGODB_DOCUMENT_JSON);
        DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
        result = mongoCollection.insert(dbObject);
        Thread.sleep(wait);
        logger.info("WriteResult: {}", result.toString());

        return dbObject;
    }

    void tearDown(){
        mongoCollection.drop();
        referencedCollection.drop();
        super.deleteRiver();
    }
}
