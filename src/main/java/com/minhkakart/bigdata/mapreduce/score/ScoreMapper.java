package com.minhkakart.bigdata.mapreduce.score;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

public class ScoreMapper extends Mapper<Text, Text, NullWritable, DoubleWritable> {
	
	CqlSession ss = null;
	String query = "Select * from [table] where id=? allow filtering";
	
	@Override
	protected void setup(Mapper<Text, Text, NullWritable, DoubleWritable>.Context context) {
        Configuration conf = context.getConfiguration();
        // Read configuration
		String contactPoint = conf.get("cassandra.contact.point", "localhost");
		String datacenter = conf.get("cassandra.datacenter", "datacenter1");
		String keyspace = conf.get("cassandra.keyspace");
		String InputTable = conf.get("cassandra.input.columnfamily");
		
		ss = new CqlSessionBuilder()
				   .addContactPoint(new InetSocketAddress(contactPoint, 9042))
				   .withLocalDatacenter(datacenter)
				   .withKeyspace(keyspace)
				   .build();
		
		query = query.replace("[table]", InputTable);
		
	}
	
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, NullWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		PreparedStatement Statement = ss.prepare(query);
		ResultSet rs = ss.execute(Statement.bind(UUID.fromString(key.toString())));
		Row row = rs.one();
		if (row != null) {
			int real = row.getInt("value_eur");
			double predicted = Double.parseDouble(value.toString());
			context.write(NullWritable.get(), new DoubleWritable(Math.abs(real-predicted)));
		}
	}

	@Override
	protected void cleanup(Mapper<Text, Text, NullWritable, DoubleWritable>.Context context) {
		if (ss != null) ss.close();
	}
	
}
