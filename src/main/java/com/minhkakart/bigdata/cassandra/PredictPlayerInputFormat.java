package com.minhkakart.bigdata.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PredictPlayerInputFormat extends InputFormat<Text, Text> {
	
	@Override
	public List<InputSplit> getSplits(JobContext context) {
		List<InputSplit> l = new ArrayList<>();
		l.add(new PlayerStatSplit());
				
		return l;
				
 	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new RecordReaderPredict();
	}
	
	@SuppressWarnings("DataFlowIssue")
    public static class RecordReaderPredict extends RecordReader<Text, Text> {
		
		private CqlSession session = null;
		Iterator<Row> iterator;
		
		Text key;
		Text value;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) {
			Configuration conf = context.getConfiguration();
	        String contactPoint = conf.get("cassandra.contact.point", "localhost");
	        String datacenter = conf.get("cassandra.datacenter", "datacenter1");
	        String keyspace = conf.get("cassandra.keyspace");
	        String kqTable = conf.get("cassandra.predicted.columnfamily");
			
	        if (session == null) {
	            session = new CqlSessionBuilder()
	                    .addContactPoint(new InetSocketAddress(contactPoint, 9042))
	                    .withLocalDatacenter(datacenter)
	                    .withKeyspace(keyspace)
	                    .build();
	        }
	        String maxSessionQuery = "Select max(session) from " + kqTable;
	        PreparedStatement sessionStatement = session.prepare(maxSessionQuery);
	        ResultSet rsSession = session.execute(sessionStatement.bind());
	        Row row = rsSession.one();
	        if (row == null || row.isNull(0))
	        	throw new RuntimeException();
	        int maxSession = row.getInt(0);

	        
	        String query = "Select * from " + kqTable + " where session=? allow filtering ";
	        PreparedStatement queryStatement = session.prepare(query);
	        ResultSet queryResult = session.execute(queryStatement.bind(maxSession));
	        iterator = queryResult.iterator();
		}

		@Override
		public boolean nextKeyValue() {
			if (iterator.hasNext()) {
				Row row = iterator.next();
				key = new Text(row.getUuid("record_id").toString());
				value = new Text(String.valueOf(row.getDouble("predicted_value")));
				
				return true;
			}
			return false;
		}

		@Override
		public Text getCurrentKey() {
			return key;
		}

		@Override
		public Text getCurrentValue() {
			return value;
		}

		@Override
		public float getProgress() {
			if (!iterator.hasNext()) return 1;
			return 0;
		}

		@Override
		public void close() {
			if (session != null)
				session.close();
		}
		
	}

}
