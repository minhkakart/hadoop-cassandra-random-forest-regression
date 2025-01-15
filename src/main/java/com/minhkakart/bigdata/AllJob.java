package com.minhkakart.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class AllJob {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.out.println("Running train job");
        int trainJobStatus = ToolRunner.run(configuration, new TrainCassandraJob(), args);
        if (trainJobStatus != 0) {
            System.err.println("Train job failed");
            System.exit(trainJobStatus);
        }
        System.err.println("Train job succeeded");
        System.out.println("Running test job");
        int testJobStatus = ToolRunner.run(configuration, new TestCassandraJob(), args);
        if (testJobStatus != 0) {
            System.err.println("Test job failed");
            System.exit(testJobStatus);
        }
        System.err.println("Test job succeeded");
        System.out.println("Running score job");
        int scoreJobStatus = ToolRunner.run(configuration, new ScoreJob(), args);
        if (scoreJobStatus != 0) {
            System.err.println("Score job failed");
            System.exit(scoreJobStatus);
        }
        System.err.println("Score job succeeded");
    }
}
