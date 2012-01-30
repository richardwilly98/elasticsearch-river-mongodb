package org.elasticsearch.river.mongodb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.common.codec.binary.Base64;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;

/*
 * GridFS Helper class
 */
public abstract class GridFSHelper {

	public static XContentBuilder serialize(GridFSDBFile file) throws IOException {

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
		
		String encodedContent = Base64.encodeBase64String(buffer.toByteArray());
		
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
					for(String key : metadata.keySet()) {
						builder.field(key, metadata.get(key));	
					}
				}
			builder.endObject();
		builder.endObject();
		
		return builder;
	}
}
