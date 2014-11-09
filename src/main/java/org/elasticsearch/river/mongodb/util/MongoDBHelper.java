/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.mongodb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

/*
 * MongoDB Helper class
 */
public abstract class MongoDBHelper {

    public static XContentBuilder serialize(GridFSDBFile file) throws IOException {

        XContentBuilder builder = XContentFactory.jsonBuilder();

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int nRead;
        byte[] data = new byte[1024];

        try (InputStream stream = file.getInputStream()) {
            while ((nRead = stream.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }

            buffer.flush();
        }

        String encodedContent = Base64.encodeBytes(buffer.toByteArray());

        // Probably not necessary...
        buffer.close();

        builder.startObject();
        builder.startObject("content");
        builder.field("_content_type", file.getContentType());
        builder.field("_title", file.getFilename());
        builder.field("_content", encodedContent);
        builder.endObject();
        builder.field("filename", file.getFilename());
        builder.field("contentType", file.getContentType());
        builder.field("md5", file.getMD5());
        builder.field("length", file.getLength());
        builder.field("chunkSize", file.getChunkSize());
        builder.field("uploadDate", file.getUploadDate());
        builder.startObject("metadata");
        DBObject metadata = file.getMetaData();
        if (metadata != null) {
            for (String key : metadata.keySet()) {
                builder.field(key, metadata.get(key));
            }
        }
        builder.endObject();
        builder.endObject();

        return builder;
    }

    public static DBObject applyExcludeFields(DBObject bsonObject, Set<String> excludeFields) {
        if (excludeFields == null) {
            return bsonObject;
        }

        DBObject filteredObject = bsonObject;
        for (String field : excludeFields) {
            if (field.contains(".")) {
                String rootObject = field.substring(0, field.indexOf("."));
                String childObject = field.substring(field.indexOf(".") + 1);
                if (filteredObject.containsField(rootObject)) {
                    Object object = filteredObject.get(rootObject);
                    if (object instanceof DBObject) {
                        DBObject object2 = (DBObject) object;
                        object2 = applyExcludeFields(object2, new HashSet<String>(Arrays.asList(childObject)));
                    }
                }
            } else {
                if (filteredObject.containsField(field)) {
                    filteredObject.removeField(field);
                }
            }
        }
        return filteredObject;
    }

    private static Set<String> getChildItems(String parent, final Set<String> fields) {
        Set<String> children = Sets.newHashSet();
        for (String field : fields) {
            if (field.startsWith(parent + ".")) {
                children.add(field.substring((parent + ".").length()));
            } else if (field.startsWith(parent)) {
                children.add(field);
            }
        }
        return children;
    }

    public static DBObject applyIncludeFields(DBObject bsonObject, final Set<String> includeFields) {
        if (includeFields == null) {
            return bsonObject;
        }

        DBObject filteredObject = new BasicDBObject();

        for (String field : bsonObject.keySet()) {
            if (includeFields.contains(field)) {
                filteredObject.put(field, bsonObject.get(field));
            }
        }

        for (String field : includeFields) {
            if (field.contains(".")) {
                String rootObject = field.substring(0, field.indexOf("."));
                // String childObject = field.substring(field.indexOf(".") + 1);
                Object object = bsonObject.get(rootObject);
                if (object instanceof DBObject) {
                    DBObject object2 = (DBObject) object;
                    // object2 = applyIncludeFields(object2, new
                    // HashSet<String>(Arrays.asList(childObject)));
                    System.out.println(getChildItems(rootObject, includeFields));
                    object2 = applyIncludeFields(object2, getChildItems(rootObject, includeFields));

                    filteredObject.put(rootObject, object2);
                }
            }
        }
        return filteredObject;
    }

    public static String getRiverVersion() {
        String version = "Undefined";
        try {
            String properties = Streams.copyToStringFromClasspath("/org/elasticsearch/river/mongodb/es-build.properties");
            Properties props = new Properties();
            props.load(new FastStringReader(properties));
            String ver = props.getProperty("version", "undefined");
            String hash = props.getProperty("hash", "undefined");
            if (!"undefined".equals(hash)) {
                hash = hash.substring(0, 7);
            }
            String timestamp = "undefined";
            String gitTimestampRaw = props.getProperty("timestamp");
            if (gitTimestampRaw != null) {
                timestamp = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC).print(Long.parseLong(gitTimestampRaw));
            }
            version = String.format("version[%s] - hash[%s] - time[%s]", ver, hash, timestamp);
        } catch (Exception ex) {
        }
        return version;

    }

    public static DBObject applyFieldFilter(DBObject object, final Set<String> includeFields, final Set<String> excludeFields) {
        if (object instanceof GridFSFile) {
            GridFSFile file = (GridFSFile) object;
            DBObject metadata = file.getMetaData();
            if (metadata != null) {
                file.setMetaData(applyFieldFilter(metadata, includeFields, excludeFields));
            }
        } else {
            object = MongoDBHelper.applyExcludeFields(object, excludeFields);
            object = MongoDBHelper.applyIncludeFields(object, includeFields);
        }
        return object;
    }
}
