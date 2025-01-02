package com.minhkakart.bigdata.cassandra;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class PlayerStatOutputFormat extends OutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        return new PlayerStatRecordWriter(taskAttemptContext.getConfiguration());
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) {
        // Do nothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
        return new PlayerStatOutputCommitter();
    }

}
