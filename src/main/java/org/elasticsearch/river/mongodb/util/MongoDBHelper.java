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
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;

/*
 * MongoDB Helper class
 */
public abstract class MongoDBHelper {

	public static XContentBuilder serialize(GridFSDBFile file)
			throws IOException {

		XContentBuilder builder = XContentFactory.jsonBuilder();

		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		int nRead;
		byte[] data = new byte[1024];

		InputStream stream = file.getInputStream();
		while ((nRead = stream.read(data, 0, data.length)) != -1) {
			buffer.write(data, 0, nRead);
		}

		buffer.flush();
		stream.close();

		String encodedContent = Base64.encodeBytes(buffer.toByteArray());

		// Probably not necessary...
		buffer.close();

		builder.startObject();
		builder.startObject("content");
		builder.field("content_type", file.getContentType());
		builder.field("title", file.getFilename());
		builder.field("content", encodedContent);
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

	public static DBObject applyExcludeFields(DBObject bsonObject,
			Set<String> excludeFields) {
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
						object2 = applyExcludeFields(object2,
								new HashSet<String>(Arrays.asList(childObject)));
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

    // Retrieves a nested value. e.g.: getNestedValue("a.b.c.d", [a:[b:[c:[d:"test"]]]]) returns "test"
    public static Object getNestedValue(String key, Map object) {
        String[] keys = key.split("\\.", 2);
        Object value = object.get(keys[0]);

        if(value == null) return null;

        if(keys.length > 1) {
            if (value instanceof Map) {
                return getNestedValue(keys[1], (Map) value);
            } else {
                return null;
            }

        } else {
            return value;
        }
    }

    // Return true if the key is in a flatten map. e.g: hasFlattenKey("a.b.c.d", [a.b.c.d.0.e:"test"]) returns true
    public static boolean hasFlattenKey(String nestedKey, Map map) {
        for(Object key: map.keySet()) {
            if(key instanceof String && ((String) key).startsWith(nestedKey)) {
                return true;
            }
        }

        return false;
    }

}
