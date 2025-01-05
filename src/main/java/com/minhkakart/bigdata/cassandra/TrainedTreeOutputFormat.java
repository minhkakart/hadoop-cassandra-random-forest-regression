package com.minhkakart.bigdata.cassandra;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class TrainedTreeOutputFormat extends OutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        return new TrainedTreeRecordWriter(taskAttemptContext.getConfiguration());
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) {
        // Do nothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
        return new EmptyOutputCommitter();
    }

}
