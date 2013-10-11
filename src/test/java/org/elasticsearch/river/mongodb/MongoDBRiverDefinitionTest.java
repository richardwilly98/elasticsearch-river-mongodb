package org.elasticsearch.river.mongodb;

import java.io.InputStream;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.mongodb.ServerAddress;

public class MongoDBRiverDefinitionTest {

    @Test
    public void testLoadMongoDBRiverDefinition() {
        try {
            RiverName riverName = new RiverName("mongodb", "mongodb-" + System.currentTimeMillis());
            InputStream in = getClass().getResourceAsStream("/org/elasticsearch/river/mongodb/test-mongodb-river-definition.json");
            RiverSettings riverSettings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), XContentHelper.convertToMap(
                    Streams.copyToByteArray(in), false).v2());
            ScriptService scriptService = null;
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(), "my-river-index", riverSettings,
                    scriptService);
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
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName.name(), "my-river-index", riverSettings,
                    scriptService);
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
}
