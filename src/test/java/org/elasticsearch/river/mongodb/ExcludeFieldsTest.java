package org.elasticsearch.river.mongodb;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

@Test
public class ExcludeFieldsTest {

    private final ESLogger logger = Loggers.getLogger(getClass());

    public void testExcludeFields() {
        try {
            Set<String> excludeFields = new HashSet<String>(Arrays.asList("lastName", "hobbies", "address.apartment"));
            // test-exclude-fields-document.json
            String mongoDocument = copyToStringFromClasspath("/org/elasticsearch/river/mongodb/test-exclude-fields-document.json");
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            logger.debug("Initial BSON object: {}", dbObject);
            DBObject filteredObject = MongoDBHelper.applyExcludeFields(dbObject, excludeFields);
            logger.debug("Filtered BSON object: {}", filteredObject);
            Assert.assertNotNull(filteredObject);
            Assert.assertFalse(filteredObject.containsField("hobbies"));
            Assert.assertTrue(filteredObject.containsField("address"));
            Assert.assertFalse(((DBObject) filteredObject.get("address")).containsField("apartment"));
        } catch (Throwable t) {
            logger.error("testExcludeFields failed", t);
            Assert.fail();
        }
    }

    public void testIncludeFields() {
        try {
            Set<String> includeFields = new HashSet<String>(Arrays.asList("lastName", "hobbies", "address.street"));
            // test-exclude-fields-document.json
            String mongoDocument = copyToStringFromClasspath("/org/elasticsearch/river/mongodb/test-exclude-fields-document.json");
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            logger.debug("Initial BSON object: {}", dbObject);
            DBObject filteredObject = MongoDBHelper.applyIncludeFields(dbObject, includeFields);
            logger.debug("Filtered BSON object: {}", filteredObject);
            Assert.assertNotNull(filteredObject);
            Assert.assertFalse(filteredObject.containsField("firstName"));
            Assert.assertTrue(filteredObject.containsField("lastName"));
            Assert.assertTrue(filteredObject.containsField("hobbies"));
            Assert.assertTrue(filteredObject.containsField("address"));
            Assert.assertTrue(((DBObject) filteredObject.get("address")).containsField("street"));
            Assert.assertFalse(((DBObject) filteredObject.get("address")).containsField("apartment"));
        } catch (Throwable t) {
            logger.error("testIncludeFields failed", t);
            Assert.fail();
        }
    }

    public void testIncludeFields204() {
        try {
            Set<String> includeFields = new HashSet<String>(Arrays.asList("system_info.userId", "place.area", "system_info.date",
                    "place.country", "system_info.removed", "system_info.premium", "place.default", "system_info.price"));
            String mongoDocument = copyToStringFromClasspath("/org/elasticsearch/river/mongodb/test-include-fields-document-204.json");
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            logger.debug("Initial BSON object: {}", dbObject);
            DBObject filteredObject = MongoDBHelper.applyIncludeFields(dbObject, includeFields);
            logger.debug("Filtered BSON object: {}", filteredObject);
            Assert.assertNotNull(filteredObject);
            Assert.assertFalse(filteredObject.containsField("gross_weight"));
            Assert.assertTrue(((DBObject) filteredObject.get("system_info")).containsField("userId"));
            Assert.assertTrue(((DBObject) filteredObject.get("place")).containsField("area"));
            Assert.assertTrue(((DBObject) filteredObject.get("system_info")).containsField("date"));
            Assert.assertTrue(((DBObject) filteredObject.get("place")).containsField("country"));
            Assert.assertTrue(((DBObject) filteredObject.get("system_info")).containsField("removed"));
            Assert.assertTrue(((DBObject) filteredObject.get("system_info")).containsField("premium"));
            Assert.assertTrue(((DBObject) filteredObject.get("place")).containsField("default"));
            Assert.assertTrue(((DBObject) filteredObject.get("system_info")).containsField("price"));
            Assert.assertFalse(filteredObject.containsField("truck_mounted_crane"));
            Assert.assertFalse(((DBObject) filteredObject.get("system_info")).containsField("photos"));
        } catch (Throwable t) {
            logger.error("testIncludeFields204 failed", t);
            Assert.fail();
        }
    }
}
