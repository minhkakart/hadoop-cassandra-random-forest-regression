package com.minhkakart.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class AllJob {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
            int trainJobStatus = ToolRunner.run(configuration, new TrainCassandraJob(), args);
            if (trainJobStatus != 0) {
                System.err.println("Train job failed");
                System.exit(trainJobStatus);
            }
            int testJobStatus = ToolRunner.run(configuration, new TestCassandraJob(), args);
            if (testJobStatus != 0) {
                System.err.println("Test job failed");
                System.exit(testJobStatus);
            }
            int scoreJobStatus = ToolRunner.run(configuration, new ScoreJob(), args);
            if (scoreJobStatus != 0) {
                System.err.println("Score job failed");
                System.exit(scoreJobStatus);
            }
    }
}
