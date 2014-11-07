package org.elasticsearch.river.mongodb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.google.common.primitives.UnsignedBytes;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryOperators;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONSerializers;

/**
 * Wrapper for BSON datatypes used in different MongoDB distros to specify the order of ops in the oplog.
 * In the official distro a {@link BSONTimestamp} is used.
 * In the TokuMX distro, a GTID is encoded in BSON as a generic {@link Binary}.
 */
public abstract class Timestamp<T extends Timestamp<T>> implements Comparable<Timestamp<T>> {

    public abstract long getTime();

    public final static class BSON extends Timestamp<BSON> {
        private final BSONTimestamp ts;

        public BSON(BSONTimestamp ts) {
            if (ts == null) {
                throw new IllegalArgumentException("ts must not be null");
            }
            this.ts = ts;
        }

        @Override
        public int compareTo(Timestamp<BSON> o) {
            return this.ts.compareTo(((BSON) o).ts);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof BSON && ts.equals(((BSON) o).ts);
        }

        @Override
        public int hashCode() {
            return ts.hashCode();
        }

        @Override
        public String toString() {
            return "Timestamp.BSON(ts=" + JSON.serialize(ts) + ")";
        }

        @Override
        public long getTime() {
            return ts.getTime() * 1000L;
        }

        @Override
        public DBObject getOplogFilter() {
            return new BasicDBObject(MongoDBRiver.OPLOG_TIMESTAMP, new BasicDBObject(QueryOperators.GTE, ts));
        }

        @Override
        public void saveFields(XContentBuilder builder) throws IOException {
            builder.field(MongoDBRiver.LAST_TIMESTAMP_FIELD, JSON.serialize(ts));
        }
    }

    public final static class GTID extends Timestamp<GTID> {
        private final byte[] gtid;
        private final Date ts;

        public GTID(byte[] gtid, Date ts) {
            if (gtid == null) {
                throw new IllegalArgumentException("gtid must not be null");
            }
            if (gtid.length != 128 / 8) { // number of octets in 128 bits
                throw new IllegalArgumentException("gtid must encode two unsigned longs (128 total bits in length)");
            }
            this.gtid = gtid;
            this.ts = ts;
        }

        @Override
        public int compareTo(Timestamp<GTID> o) {
            return UnsignedBytes.lexicographicalComparator().compare(this.gtid, ((GTID) o).gtid);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof GTID && Arrays.equals(gtid, ((GTID) o).gtid);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(gtid);
        }

        @Override
        public String toString() {
            return "Timestamp.GTID(ts=" + JSON.serialize(ts) + ", gtid=" + JSONSerializers.getStrict().serialize(gtid) + ")";
        }

        @Override
        public long getTime() {
            return ts.getTime();
        }

        @Override
        public DBObject getOplogFilter() {
            return new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, new BasicDBObject(QueryOperators.GTE, gtid));
        }

        @Override
        public void saveFields(XContentBuilder builder) throws IOException {
            builder.field(MongoDBRiver.LAST_TIMESTAMP_FIELD, JSON.serialize(ts));
            builder.field(MongoDBRiver.LAST_GTID_FIELD, JSONSerializers.getStrict().serialize(gtid));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static int compare(Timestamp oplogTimestamp, Timestamp startTimestamp) {
        return oplogTimestamp.compareTo(startTimestamp);
    }

    /** Parse timestamp field(s) on an oplog entry. */
    public static Timestamp<?> on(DBObject entry) {
        return on(entry.toMap(), false);
    }

    /** Parse last timestamp field(s) from river source metadata. */
    public static Timestamp<?> on(Map<String, Object> map) {
        return on(map, true);
    }

    private static Timestamp<?> on(@SuppressWarnings("rawtypes") Map map, boolean meta) {
        String tsField = meta ? MongoDBRiver.LAST_TIMESTAMP_FIELD : MongoDBRiver.OPLOG_TIMESTAMP;
        Object timestamp = map.get(tsField);
        if (timestamp == null) {
            return null;
        }
        if (timestamp instanceof String) {
            timestamp = JSON.parse((String) timestamp);
        }
        if (timestamp instanceof BSONTimestamp) {
            BSON result = new Timestamp.BSON((BSONTimestamp) timestamp);
            return result;
        }
        if (timestamp instanceof Date) {
            String gtidField = meta ? MongoDBRiver.LAST_GTID_FIELD : MongoDBRiver.MONGODB_ID_FIELD;
            Object id = map.get(gtidField);
            GTID result = null;
            if (id == null) {
                throw new IllegalStateException("Missing property: " + gtidField);
            }
            if (id instanceof String) {
                id = JSON.parse((String) id);
            }
            if (id instanceof Binary) {
                result = new Timestamp.GTID(((Binary) id).getData(), (Date) timestamp);
            } else if (id instanceof byte[]) {
                result = new Timestamp.GTID((byte[]) id, (Date) timestamp);
            }
            if (result == null) {
                throw new IllegalStateException("Unable to parse " + gtidField
                        + " " + id + " of type " + id.getClass());
            }
            return result;
        }
        throw new IllegalStateException("Unable to parse " + tsField
                + " " + timestamp + " of type " + timestamp.getClass());
    }

    public abstract DBObject getOplogFilter();

    public abstract void saveFields(XContentBuilder builder) throws IOException;
}
