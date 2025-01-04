package com.minhkakart.bigdata;

import com.minhkakart.bigdata.cassandra.PlayerStatInputFormat;
import com.minhkakart.bigdata.cassandra.TestTreeOutputFormat;
import com.minhkakart.bigdata.mapreduce.test.RandomForestTestMapper;
import com.minhkakart.bigdata.mapreduce.test.RandomForestTestReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TestJob(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();


		Job job = Job.getInstance(conf, "Random Forest Cassandra");

		job.setJarByClass(TestJob.class);
		job.setMapperClass(RandomForestTestMapper.class);
		job.setReducerClass(RandomForestTestReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(PlayerStatInputFormat.class);
		job.setOutputFormatClass(TestTreeOutputFormat.class);


		return job.waitForCompletion(true) ? 0 : 1;
	}
}
