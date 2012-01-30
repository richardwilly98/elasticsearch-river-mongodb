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
