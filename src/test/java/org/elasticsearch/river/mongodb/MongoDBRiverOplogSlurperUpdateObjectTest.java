package org.elasticsearch.river.mongodb;

import com.mongodb.BasicDBObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;

public class MongoDBRiverOplogSlurperUpdateObjectTest {

    @Test
    public void testHasAnyFieldOf() {
        try {
            BasicDBObject object = new BasicDBObject();
            object.put("parent.child", "Hello");
            object.put("parent2", "World");
            OplogSlurper.UpdateObject updObject = new OplogSlurper.UpdateObject(object);

            HashSet<String> testAgainst = new HashSet<>();
            testAgainst.add("parent");

            Assert.assertTrue(updObject.hasAnyFieldOf(testAgainst));

            testAgainst.clear();
            testAgainst.add("parent2");

            Assert.assertTrue(updObject.hasAnyFieldOf(testAgainst));

            testAgainst.clear();
            testAgainst.add("paren");

            Assert.assertFalse(updObject.hasAnyFieldOf(testAgainst));
        } catch (Throwable t) {
            Assert.fail("testHasAnyFieldOf failed", t);
        }
    }

    @Test
    public void testHasNoFieldsExcept() {
        try {
            BasicDBObject object = new BasicDBObject();
            object.put("parent.child", "Hello");
            object.put("parent2", "World");
            OplogSlurper.UpdateObject updObject = new OplogSlurper.UpdateObject(object);

            HashSet<String> testAgainst = new HashSet<>();
            testAgainst.add("parent");
            testAgainst.add("parent2");
            testAgainst.add("parent3");

            Assert.assertTrue(updObject.hasNoFieldsExcept(testAgainst));

            testAgainst.clear();
            testAgainst.add("parent");

            Assert.assertFalse(updObject.hasNoFieldsExcept(testAgainst));
        } catch (Throwable t) {
            Assert.fail("testHasNoFieldsExcept failed", t);
        }
    }

    @Test
    public void testGetAllFields() {
        try {
            BasicDBObject object = new BasicDBObject();

            BasicDBObject grandchildren = new BasicDBObject();
            grandchildren.append("gc1", true);
            grandchildren.append("gc2", true);

            BasicDBObject children = new BasicDBObject();
            children.append("child1", true);
            children.append("child2", grandchildren);

            object.put("parent", children);
            object.put("$set", new BasicDBObject("set_field", true));
            object.put("$inc", new BasicDBObject("root", 1l));
            object.put("root", true);

            OplogSlurper.UpdateObject updObject = new OplogSlurper.UpdateObject(object);

            HashSet<String> expected = new HashSet<>();
            expected.add("parent.child1");
            expected.add("parent.child2.gc1");
            expected.add("parent.child2.gc2");
            expected.add("set_field");
            expected.add("root");

            Assert.assertEquals(expected, updObject.getAllFields());
        } catch (Throwable t) {
            Assert.fail("testGetAllFields failed", t);
        }
    }

}
