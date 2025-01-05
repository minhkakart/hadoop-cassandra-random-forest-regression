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
        Job job = Job.getInstance(conf, "Random Forest Cassandra Training");

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
