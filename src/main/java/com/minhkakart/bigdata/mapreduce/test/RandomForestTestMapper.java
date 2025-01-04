package com.minhkakart.bigdata.mapreduce.test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.minhkakart.bigdata.algorithm.DecisionTree;
import com.minhkakart.bigdata.algorithm.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RandomForestTestMapper extends Mapper<Text, Text, Text, DoubleWritable> {
    List<DecisionTree> forest = new ArrayList<>(100);

    @Override
    protected void setup(Mapper<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        // Read configuration
		String contactPoint = conf.get("cassandra.contact.point", "localhost");
		String datacenter = conf.get("cassandra.datacenter", "datacenter1");
		String keyspace = conf.get("cassandra.keyspace");
		String trainTable = conf.get("cassandra.output.columnfamily");

        // Connect to Cassandra
		CqlSession session = new CqlSessionBuilder()
			   .addContactPoint(new InetSocketAddress(contactPoint, 9042))
			   .withLocalDatacenter(datacenter)
			   .withKeyspace(keyspace)
			   .build();

        // Reachieved last session id
		String lastSessionIdCql = "SELECT max(session) FROM " + keyspace + "." + trainTable;
        PreparedStatement preparedLastSession = session.prepare(lastSessionIdCql);
		ResultSet resultSet = session.execute(preparedLastSession.bind());
		Row lastSessionRow = resultSet.one();

        assert lastSessionRow != null;
        int sessionId = Integer.parseInt(conf.get("cassandra.output.session", String.valueOf(lastSessionRow.getInt(0))));

        String treesQuery = "SELECT * FROM " + trainTable + " WHERE session = ? ALLOW FILTERING;";
		PreparedStatement rangeStatement = session.prepare(treesQuery);
		ResultSet rs = session.execute(rangeStatement.bind(sessionId));
        while (rs.iterator().hasNext()){
            DecisionTree tree = new DecisionTree();
            tree.setRoot(DecisionTree.getSerializer().fromJson(rs.iterator().next().getString("value"), Node.class));
            forest.add(tree);
        }

        session.close();
    }

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double[] features = Arrays.stream(value.toString().split(",")).mapToDouble(Double::parseDouble).toArray();

        for (DecisionTree decisionTree : forest) {
            double predicted = decisionTree.predict(features);
            context.write(key, new DoubleWritable(predicted));
        }
    }
}
