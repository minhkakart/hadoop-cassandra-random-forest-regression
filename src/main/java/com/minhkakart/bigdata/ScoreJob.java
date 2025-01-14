package com.minhkakart.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.minhkakart.bigdata.cassandra.PredictPlayerInputFormat;
import com.minhkakart.bigdata.cassandra.TrainedTreeOutputFormat;
import com.minhkakart.bigdata.mapreduce.score.ScoreMapper;
import com.minhkakart.bigdata.mapreduce.score.ScoreReducer;

public class ScoreJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ScoreJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Random Forest Cassandra Training");

        job.setJarByClass(ScoreJob.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);


        job.setInputFormatClass(PredictPlayerInputFormat.class);
        job.setOutputFormatClass(TrainedTreeOutputFormat.class);

//        FileOutputFormat.setOutputPath(job, new Path("D:/btl9"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
