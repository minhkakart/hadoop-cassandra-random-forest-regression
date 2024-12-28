package com.minhkakart.bigdata;

import com.minhkakart.bigdata.mapreduce.RandomForestTrainMapper;
import com.minhkakart.bigdata.mapreduce.RandomForestTrainReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrainJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new TrainJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");
        String output = conf.get("output");
        /* Configuration parameters
        String n_estimators = conf.get("n_estimators");
        String max_depth = conf.get("max_depth");
        String max_features = conf.get("max_features");
        String min_samples_split = conf.get("min_samples_split");
        */
        
        Job job = Job.getInstance(conf, "Random Forest");

        job.setJarByClass(TrainJob.class);
        job.setMapperClass(RandomForestTrainMapper.class);
        job.setReducerClass(RandomForestTrainReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}