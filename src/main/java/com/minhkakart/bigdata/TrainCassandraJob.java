package com.minhkakart.bigdata;

import com.minhkakart.bigdata.cassandra.PlayerStatInputFormat;
import com.minhkakart.bigdata.cassandra.TrainedTreeOutputFormat;
import com.minhkakart.bigdata.mapreduce.train.RandomForestTrainMapper;
import com.minhkakart.bigdata.mapreduce.train.RandomForestTrainReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrainCassandraJob extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TrainCassandraJob(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
        /* Configuration parameters
        // Configurations for Random Forest
        String n_estimators = conf.get("n_estimators");
        String max_depth = conf.get("max_depth");
        String max_features = conf.get("max_features");
        String min_samples_split = conf.get("min_samples_split");
        
        // Configurations for Cassandra
        String cassandra_contact_point = conf.get("cassandra.contact.point");
        String cassandra_keyspace = conf.get("cassandra.keyspace");
        String cassandra_datacenter = conf.set("cassandra.datacenter", "datacenter1");
        String cassandra_input_columnfamily = conf.get("cassandra.input.columnfamily");
        String cassandra_output_columnfamily = conf.get("cassandra.output.columnfamily");
        */

		Job job = Job.getInstance(conf, "Random Forest Cassandra");

		job.setJarByClass(TrainCassandraJob.class);
		job.setMapperClass(RandomForestTrainMapper.class);
		job.setReducerClass(RandomForestTrainReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(PlayerStatInputFormat.class);
		job.setOutputFormatClass(TrainedTreeOutputFormat.class);


		return job.waitForCompletion(true) ? 0 : 1;
	}
}
