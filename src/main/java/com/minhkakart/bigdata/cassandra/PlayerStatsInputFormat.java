package com.minhkakart.bigdata.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("SpellCheckingInspection")
public class PlayerStatsInputFormat extends InputFormat<LongWritable, Text> {
    private static final int LIMIT = 1000;

    @Override
    public List<InputSplit> getSplits(JobContext context) {
        List<InputSplit> splits = new ArrayList<>();
        Configuration conf = context.getConfiguration();
        String contactPoint = conf.get("cassandra.contact.point", "localhost");
        String keyspace = conf.get("cassandra.input.keyspace");
        String datacenter = conf.get("cassandra.input.datacenter", "datacenter1");
        String table = conf.get("cassandra.input.columnfamily");

        // Connect to Cassandra
        try (CqlSession session = new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress(contactPoint, 9042))
                .withLocalDatacenter(datacenter)
                .withKeyspace(keyspace)
                .build()) {

            // Count the number of rows in the table
            String countQuery = "SELECT COUNT(*) FROM " + table;
            PreparedStatement countStatement = session.prepare(countQuery);
            ResultSet count = session.execute(countStatement.bind());
            long numRows = Objects.requireNonNull(count.one()).getLong(0);

            // If the table is empty, return an empty split
            if (numRows == 0) {
                return splits;
            }

            // Calculate the number of splits based on the number of rows
            int numSplits = (int) Math.ceil((double) numRows / LIMIT);

            // Create splits based on the number of rows
            long currentToken = Long.MIN_VALUE;
            for (int i = 0; i < numSplits; i++) {
                String tokenQuery = "SELECT token(id) FROM " + table + " LIMIT ? ALLOW FILTERING";
                PreparedStatement tokenStatement = session.prepare(tokenQuery);
                ResultSet tokens = session.execute(tokenStatement.bind(LIMIT));
                List<Row> rows = tokens.all();
                long nextToken = rows.get(rows.size() - 1).getLong(0);
                splits.add(new PlayerStatsSplit(currentToken));
                currentToken = nextToken;
            }

        }

        return splits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new CassandraRecordReader();
    }

    /**
     * InputSplit for PlayerStats partition
     */
    public static class PlayerStatsSplit extends InputSplit implements Writable {
        private long token;

        public PlayerStatsSplit() {
            this.token = Long.MIN_VALUE;
        }

        public PlayerStatsSplit(long token) {
            this.token = token;
        }

        @Override
        public long getLength() {
            return 1;
        }

        @Override
        public String[] getLocations() {
            return new String[0];
        }

        public long getToken() {
            return token;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(token);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            token = in.readLong();
        }
    }

    /**
     * RecordReader for Cassandra rows
     */
    public static class CassandraRecordReader extends RecordReader<LongWritable, Text> {
        private Iterator<Row> iterator;
        private final Text currentKey = new Text();
        private final Text currentValue = new Text();
        private long loadedItems;
        private long totalItems;
        private long token;

        private CqlSession session;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
            Configuration conf = context.getConfiguration();
            String contactPoint = conf.get("cassandra.contact.point", "localhost");
            String keyspace = conf.get("cassandra.input.keyspace");
            String datacenter = conf.get("cassandra.input.datacenter", "datacenter1");
            String table = conf.get("cassandra.input.columnfamily");

            // Connect to Cassandra
            session = new CqlSessionBuilder()
                    .addContactPoint(new InetSocketAddress(contactPoint, 9042))
                    .withLocalDatacenter(datacenter)
                    .withKeyspace(keyspace)
                    .build();

            // Execute the query for the specific token range
            long token = ((PlayerStatsSplit) split).getToken();
            this.token = token;
            String rangeQuery = "SELECT * FROM " + table + " WHERE token(id) > ? LIMIT ?";
            PreparedStatement rangeStatement = session.prepare(rangeQuery);
            ResultSet rs = session.execute(rangeStatement.bind(token, LIMIT));

            // Prepare the iterator
            this.totalItems = rs.getAvailableWithoutFetching();
            this.iterator = rs.iterator();
        }

        @Override
        public boolean nextKeyValue() {
            if (iterator != null && iterator.hasNext()) {
                Row row = iterator.next();
                PlayerStat playerStat = new PlayerStat(row);
                currentKey.set(playerStat.getId());
                currentValue.set(playerStat.toCsv());
                loadedItems++;
                return true;
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() {
            return new LongWritable(loadedItems);
        }

        @Override
        public Text getCurrentValue() {
            return currentValue;
        }

        @Override
        public float getProgress() {
            if (iterator == null || !iterator.hasNext() || totalItems == 0 || loadedItems >= totalItems) {
                return 1.0f;
            }
            return (float) loadedItems / totalItems;
        }

        @Override
        public void close() {
            if (session != null) session.close();
        }

        public long getToken() {
            return token;
        }

        public long getLoadedItems() {
            return loadedItems;
        }

        public long getTotalItems() {
            return totalItems;
        }
    }
}
