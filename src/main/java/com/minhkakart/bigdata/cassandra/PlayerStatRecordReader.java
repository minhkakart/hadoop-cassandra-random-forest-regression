package com.minhkakart.bigdata.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.net.InetSocketAddress;
import java.util.Iterator;

/**
 * RecordReader for Cassandra rows
 */
@SuppressWarnings("unused")
public class PlayerStatRecordReader extends RecordReader<LongWritable, Text> {
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
        String datacenter = conf.get("cassandra.datacenter", "datacenter1");
        String keyspace = conf.get("cassandra.keyspace");
        String table = conf.get("cassandra.input.columnfamily");

        if (keyspace == null || table == null) {
            throw new IllegalArgumentException("Cassandra input configuration missing keyspace or table.");
        }

        // Connect to Cassandra
        session = new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress(contactPoint, 9042))
                .withLocalDatacenter(datacenter)
                .withKeyspace(keyspace)
                .build();

        // Execute the query for the specific token range
        long token = ((PlayerStatSplit) split).getToken();
        this.token = token;
        String rangeQuery = "SELECT * FROM " + table + " WHERE token(id) > ? LIMIT ?";
        PreparedStatement rangeStatement = session.prepare(rangeQuery);
        ResultSet rs = session.execute(rangeStatement.bind(token, PlayerStatInputFormat.LIMIT));

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
