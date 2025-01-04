package com.minhkakart.bigdata.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PlayerStatInputFormat extends InputFormat<Text, Text> {
	public static final int LIMIT = 1000;
	private static final int MAX_ITEMS = 5000;

	@Override
	public List<InputSplit> getSplits(JobContext context) {
		List<InputSplit> splits = new ArrayList<>();
		Configuration conf = context.getConfiguration();
		String contactPoint = conf.get("cassandra.contact.point", "localhost");
		String keyspace = conf.get("cassandra.keyspace");
		String datacenter = conf.get("cassandra.datacenter", "datacenter1");
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
			numRows = Math.min(numRows, MAX_ITEMS);

			// If the table is empty, return an empty split
			if (numRows == 0) {
				return splits;
			}

			// Calculate the number of splits based on the number of rows
			int numSplits = (int) Math.ceil((double) numRows / LIMIT);

			// Create splits based on the number of rows
			long currentToken = Long.MIN_VALUE;
			for (int i = 0; i < numSplits; i++) {
				String tokenQuery = "SELECT token(id) FROM " + table + " WHERE token(id) > ?  LIMIT ? ALLOW FILTERING";
				PreparedStatement tokenStatement = session.prepare(tokenQuery);
				ResultSet tokens = session.execute(tokenStatement.bind(currentToken, LIMIT));
				List<Row> rows = tokens.all();
				long nextToken = rows.get(rows.size() - 1).getLong(0);
				splits.add(new PlayerStatSplit(currentToken));
				currentToken = nextToken;
			}

		}

		return splits;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new PlayerStatRecordReader();
	}

}
