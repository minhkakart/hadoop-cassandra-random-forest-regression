package com.minhkakart.bigdata.cassandra;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class TestTreeOutputFormat extends OutputFormat<Text, DoubleWritable> {

    @Override
    public RecordWriter<Text, DoubleWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        return new TestTreesRecordWriter(taskAttemptContext.getConfiguration());
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
        return new EmptyOutputCommitter();
    }
}
