package org.elasticsearch.river.mongodb;

import java.io.InputStream;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

public class MongoDBRiverDefinitionTest {

    @Test
    public void testLoadMongoDBRiverSimpleDefinition() {
        try {
            RiverName riverName = new RiverName("mongodb", "mongodb-" + System.currentTimeMillis());
            InputStream in = getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-simple-definition.json");
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(),
                    RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverSettings, scriptService);
            Assert.assertNotNull(definition);
            Assert.assertEquals("mydb", definition.getMongoDb());
            Assert.assertEquals("mycollection", definition.getMongoCollection());
            Assert.assertEquals("myindex", definition.getIndexName());

            // Test default bulk values
            Assert.assertEquals(MongoDBRiverDefinition.DEFAULT_BULK_ACTIONS, definition.getBulk().getBulkActions());
            Assert.assertEquals(MongoDBRiverDefinition.DEFAULT_CONCURRENT_REQUESTS, definition.getBulk().getConcurrentRequests());
            Assert.assertEquals(MongoDBRiverDefinition.DEFAULT_BULK_SIZE, definition.getBulk().getBulkSize());
            Assert.assertEquals(MongoDBRiverDefinition.DEFAULT_FLUSH_INTERVAL, definition.getBulk().getFlushInterval());
            Assert.assertFalse(definition.isSkipInitialImport());
            Assert.assertFalse(definition.isStoreStatistics());

        } catch (Throwable t) {
            Assert.fail("testLoadMongoDBRiverSimpleDefinition failed", t);
        }
    }

    @Test
    public void testLoadMongoDBRiverDefinition() {
        try {
            RiverName riverName = new RiverName("mongodb", "mongodb-" + System.currentTimeMillis());
            InputStream in = getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-definition.json");
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(),
                    RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverSettings, scriptService);
            Assert.assertNotNull(definition);
            Assert.assertEquals("mycollection", definition.getIncludeCollection());
            Assert.assertTrue(definition.getParentTypes().contains("parent1"));
            Assert.assertTrue(definition.getParentTypes().contains("parent2"));
            Assert.assertFalse(definition.getParentTypes().contains("parent3"));
            Assert.assertTrue(definition.isAdvancedTransformation());
            Assert.assertEquals("mydatabase", definition.getMongoDb());
            Assert.assertEquals("mycollection", definition.getMongoCollection());
            Assert.assertEquals("myindex", definition.getIndexName());
            Assert.assertEquals(0, definition.getSocketTimeout());
            Assert.assertEquals(11000, definition.getConnectTimeout());
            Assert.assertEquals(riverName.getName(), definition.getRiverName());
            Assert.assertFalse(definition.isStoreStatistics());
            
            // Test bulk
            Assert.assertEquals(500, definition.getBulk().getBulkActions());
            Assert.assertEquals(40, definition.getBulk().getConcurrentRequests());

        } catch (Throwable t) {
            Assert.fail("testLoadMongoDBRiverDefinition failed", t);
        }
    }

    @Test
    public void testLoadMongoDBRiverNewDefinition() {
        try {
            RiverName riverName = new RiverName("mongodb", "mongodb-" + System.currentTimeMillis());
            InputStream in = getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-new-definition.json");
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(),
                    RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverSettings, scriptService);
            Assert.assertNotNull(definition);
            Assert.assertEquals("mycollection", definition.getIncludeCollection());
            Assert.assertTrue(definition.getParentTypes().contains("parent1"));
            Assert.assertTrue(definition.getParentTypes().contains("parent2"));
            Assert.assertFalse(definition.getParentTypes().contains("parent3"));
            Assert.assertTrue(definition.isAdvancedTransformation());
            Assert.assertEquals("mydatabase", definition.getMongoDb());
            Assert.assertEquals("mycollection", definition.getMongoCollection());
            Assert.assertEquals("myindex", definition.getIndexName());
            Assert.assertEquals(0, definition.getSocketTimeout());
            Assert.assertEquals(11000, definition.getConnectTimeout());
            Assert.assertEquals(riverName.getName(), definition.getRiverName());

            // actions: 500
            // size: "20mb",
            // concurrent_requests: 40,
            // flush_interval: "50ms"

            // Test bulk
            Assert.assertEquals(500, definition.getBulk().getBulkActions());
            Assert.assertEquals(40, definition.getBulk().getConcurrentRequests());
            Assert.assertEquals(ByteSizeValue.parseBytesSizeValue("20mb"), definition.getBulk().getBulkSize());
            Assert.assertEquals(TimeValue.timeValueMillis(50), definition.getBulk().getFlushInterval());

        } catch (Throwable t) {
            Assert.fail("testLoadMongoDBRiverDefinition failed", t);
        }
    }

    @Test
    public void testLoadMongoDBRiverDefinitionIssue159() {
        try {
            RiverName riverName = new RiverName("mongodb", "mongodb-" + System.currentTimeMillis());
            InputStream in = getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-definition-159.json");
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(),
                    RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverSettings, scriptService);
            Assert.assertNotNull(definition);

            Assert.assertEquals(2, definition.getMongoServers().size());
            ServerAddress serverAddress = definition.getMongoServers().get(0);
            Assert.assertEquals(serverAddress.getHost(), "127.0.0.1");
            Assert.assertEquals(serverAddress.getPort(), MongoDBRiverDefinition.DEFAULT_DB_PORT);
            serverAddress = definition.getMongoServers().get(1);
            Assert.assertEquals(serverAddress.getHost(), "localhost");
            Assert.assertEquals(serverAddress.getPort(), MongoDBRiverDefinition.DEFAULT_DB_PORT);

        } catch (Throwable t) {
            Assert.fail("testLoadMongoDBRiverDefinitionIssue159 failed", t);
        }
    }

    @Test
    public void testLoadMongoDBRiverDefinitionIssue167() {
        try {
            RiverName riverName = new RiverName("mongodb", "mongodb-" + System.currentTimeMillis());
            InputStream in = getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-definition-167.json");
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(),
                    RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverSettings, scriptService);
            Assert.assertNotNull(definition);
            Assert.assertTrue(definition.isSkipInitialImport());
            Assert.assertTrue(definition.isStoreStatistics());
        } catch (Throwable t) {
            Assert.fail("testLoadMongoDBRiverDefinitionIssue167 failed", t);
        }
    }

    @Test
    public void testLoadMongoDBRiverDefinitionIssue177() {
        try {
            RiverName riverName = new RiverName("mongodb", "mongodb-" + System.currentTimeMillis());
            InputStream in = getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-definition-177.json");
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(),
                    RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverSettings, scriptService);
            Assert.assertNotNull(definition);
            Assert.assertTrue(definition.isImportAllCollections());
            Assert.assertTrue(definition.isDropCollection());
        } catch (Throwable t) {
            Assert.fail("testLoadMongoDBRiverDefinitionIssue177 failed", t);
        }
    }

    @Test
    public void testLoadMongoDBRiverDefinitionStoreStatistics() {
        try {
            RiverName riverName = new RiverName("mongodb", "mongodb-" + System.currentTimeMillis());
            InputStream in = getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-definition-store-statistics.json");
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(),
                    RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverSettings, scriptService);
            Assert.assertNotNull(definition);
            Assert.assertTrue(definition.isStoreStatistics());
            Assert.assertEquals(definition.getStatisticsIndexName(), "archive-stats");
            Assert.assertEquals(definition.getStatisticsTypeName(), "dummy-stats");
        } catch (Throwable t) {
            Assert.fail("testLoadMongoDBRiverDefinitionStoreStatistics failed", t);
        }
    }

    @Test
    public void parseFilter() {
        String filter = "{\"o.lang\":\"de\"}";
        BasicDBObject bsonFilter = (BasicDBObject) JSON.parse(filter);
        String filterNoPrefix = MongoDBRiverDefinition.removePrefix("o.", filter);
        Assert.assertNotNull(filterNoPrefix);

        BasicDBObject bsonFilterNoPrefix = (BasicDBObject) JSON.parse(filterNoPrefix);
        Assert.assertNotNull(bsonFilterNoPrefix);

        // call a second time trimPrefix has no effect
        String filterNoPrefix2 = MongoDBRiverDefinition.removePrefix("o.", bsonFilterNoPrefix.toString());
        Assert.assertNotNull(filterNoPrefix2);
        BasicDBObject bsonFilterNoPrefix2 = (BasicDBObject) JSON.parse(filterNoPrefix2);
        Assert.assertEquals(bsonFilterNoPrefix, bsonFilterNoPrefix2);

        String filterWithPrefix = MongoDBRiverDefinition.addPrefix("o.", filterNoPrefix);
        BasicDBObject bsonFilterWithPrefix = (BasicDBObject) JSON.parse(filterWithPrefix);
        Assert.assertNotNull(bsonFilterWithPrefix);
        // trimPrefix + addPrefix returns the original bson
        Assert.assertEquals(bsonFilter, bsonFilterWithPrefix);
    }

}
