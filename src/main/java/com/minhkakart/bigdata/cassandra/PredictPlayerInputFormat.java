package com.minhkakart.bigdata.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class PredictPlayerInputFormat extends InputFormat<Text, Text> {

	
	
	
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		List<InputSplit> l = new ArrayList<>();
		l.add(new InputSplit() {
			
			@Override
			public String[] getLocations() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public long getLength() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return 0;
			}
		});
				
		return l;
				
 	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static class RecoredReaderPredict extends RecordReader<Text, Text> {
		
		private CqlSession session = null;
		Iterator<Row> iterator;
		
		Text key;
		Text value;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
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
	        String session_query = "Select max(session) from " + kqTable;
	        PreparedStatement sessionStatement = session.prepare(session_query);
	        ResultSet rsession = session.execute(sessionStatement.bind());
	        Row row = rsession.one();
	        if (row == null || row.isNull(0))
	        	throw new RuntimeException();
	        long max_session = row.getLong(0);

	        
	        String query = "Select * from " + kqTable + " where session=? allow filtering ";
	        PreparedStatement queryStatement = session.prepare(session_query);
	        ResultSet querysession = session.execute(queryStatement.bind(max_session));
	        iterator = querysession.iterator();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (iterator.hasNext()) {
				Row row = iterator.next();
				key = new Text(row.getUuid("record_id").toString());
				value = new Text(String.valueOf(row.getDouble("predicted_value")));
				
				return true;
			}
				
			return false;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (!iterator.hasNext()) return 1;
			return 0;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			if (session != null)
				session.close();
		}
		
	}

}
