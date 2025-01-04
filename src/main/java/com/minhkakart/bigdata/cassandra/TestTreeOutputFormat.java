package com.minhkakart.bigdata.cassandra;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class TestTreeOutputFormat extends OutputFormat<Text, DoubleWritable> {

    @Override
    public RecordWriter<Text, DoubleWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TestTreesRecordWriter(taskAttemptContext.getConfiguration());
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TrainedTreeOutputCommitter();
    }
}
