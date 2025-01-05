package com.minhkakart.bigdata.cassandra;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EmptyOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) {
        // Do nothing
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) {
        // Do nothing
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) {
        // Do nothing
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) {
        // Do nothing
    }
}
