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

/*
 * Helper class provided to deal with local.oplog.rs collection
 */
public abstract class OplogHelper {

	/**
	 * returns a database name from FQ namespace. Assumes database name never
	 * has "." in it.
	 * 
	 * @param namespace
	 * @return
	 */
	public static String getDatabaseNameFromNamespace(String namespace) {
		String[] parts = namespace.split("\\.");
		if (parts == null || parts.length == 1) {
			return null;
		}
		String databaseName = parts[0];
		return databaseName;
	}

	/**
	 * returns a collection name from FQ namespace. Assumes database name never
	 * has "." in it.
	 * 
	 * @param namespace
	 * @return
	 */
	public static String getCollectionFromNamespace(String namespace) {
		String[] parts = namespace.split("\\.");
		if (parts == null || parts.length == 1) {
			return null;
		}
		String collection = null;
		if (parts.length == 2) {
			collection = parts[1];
		} else {
			collection = namespace.substring(parts[0].length() + 1);
		}
		return collection;
	}
}
