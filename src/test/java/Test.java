import com.minhkakart.bigdata.cassandra.PlayerStatsInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.FileNotFoundException;

@SuppressWarnings("ALL")
public class Test {
    public static void main(String[] args) throws FileNotFoundException {
        /*
        List<DecisionTree> forest = new ArrayList<>(100);
        String forestPath = "E:/TLU_Subject/Ki_7/BigData/output/btl-test/part-r-00000";
        BufferedReader reader = new BufferedReader(new FileReader(forestPath));
        reader.lines().forEach(line -> {
            String[] split = line.split("\t");
            DecisionTree tree = new DecisionTree();
            tree.setRoot(DecisionTree.getSerializer().fromJson(split[1], Node.class));
            forest.add(tree);
        });
        double[][] testData = new double[][]{
                new double[]{33, 187, 83, 94, 5, 4, 93, 81, 89, 35, 79, 77000000},
                new double[]{31, 170, 72, 94, 5, 4, 91, 88, 96, 32, 61, 110500000},
                new double[]{26, 175, 68, 93, 5, 5, 84, 83, 95, 32, 59, 118500000},
                new double[]{27, 181, 70, 92, 4, 5, 86, 92, 87, 60, 78, 102000000},
                new double[]{32, 184, 82, 91, 4, 3, 63, 71, 71, 91, 84, 51000000},
                new double[]{31, 182, 86, 91, 5, 4, 90, 79, 88, 52, 85, 80000000},
                new double[]{32, 172, 66, 91, 4, 4, 76, 90, 91, 70, 67, 67000000},
                new double[]{27, 173, 74, 91, 4, 4, 82, 86, 94, 35, 67, 93000000},
                new double[]{32, 187, 78, 90, 3, 3, 48, 65, 62, 89, 84, 44000000},
                new double[]{28, 183, 76, 90, 4, 5, 82, 89, 82, 74, 69, 76500000},
                new double[]{29, 184, 80, 90, 4, 4, 89, 75, 85, 41, 82, 77000000},
        };


        double[] actual = new double[testData.length];
        double[] predictedArr = new double[testData.length];
        
        for (int i = 0; i < testData.length; i++) {
            actual[i] = testData[i][testData[0].length - 1];
            double[] predicted = new double[forest.size()];
            for (int j = 0; j < forest.size(); j++) {
                predicted[j] = forest.get(j).predict(testData[i]);
            }
            predictedArr[i] = Calculator.mean(predicted);
        }

        for (double predicted : predictedArr) {
            System.out.println((int) predicted);
        }

        System.out.println("Root mean square error: " + Calculator.rootMeanSquareError(actual, predictedArr));
        System.out.println("Mean absolute error: " + Calculator.meanAbsoluteError(actual, predictedArr));
        System.out.println("Nash-Sutcliffe efficiency: " + Calculator.nashSutcliffeEfficiency(actual, predictedArr));
        System.out.println("Coefficient of determination: " + Calculator.coefficientOfDetermination(actual, predictedArr));*/

        /*
        int LIMIT = 1000;

        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("nodemaster", 9042))
                .withLocalDatacenter("datacenter1")
                .withKeyspace("bigdata")
                .build();

        // Count the number of rows in the table
        String countQuery = "SELECT COUNT(*) FROM player_stats";
        PreparedStatement countStatement = session.prepare(countQuery);
        ResultSet count = session.execute(countStatement.bind());
        long numRows = Objects.requireNonNull(count.one()).getLong(0);

        // If the table is empty, return an empty split
        if (numRows == 0) {
            System.out.println("Empty table");
        } else {
            // Calculate the number of splits based on the number of rows
            int numSplits = (int) Math.ceil((double) numRows / LIMIT);

            // Create splits based on the number of rows
            int currentPartition = 0;
            long currentToken = Long.MIN_VALUE;
            String tokenQuery = "SELECT token(id) as \"token\" FROM player_stats WHERE token(id) > ? LIMIT ? ALLOW FILTERING";
            for (int i = 0; i < numSplits; i++) {
                PreparedStatement tokenStatement = session.prepare(tokenQuery);
                ResultSet tokens = session.execute(tokenStatement.bind(currentToken, LIMIT));
                List<Row> rows = tokens.all();
                currentToken = rows.get(rows.size() - 1).getLong("token");
                System.out.println(String.format("Partition %d: %d rows, token: %s", i, rows.size(), currentToken));
            }
        }

        session.close();
        */


        PlayerStatsInputFormat.PlayerStatsSplit split = new PlayerStatsInputFormat.PlayerStatsSplit();
        Configuration conf = new Configuration();
        conf.set("cassandra.contact.point", "nodemaster");
        conf.set("cassandra.input.keyspace", "bigdata");
        conf.set("cassandra.input.datacenter", "datacenter1");
        TaskAttemptID taskAttemptID = new TaskAttemptID("job", 1, true, 1, 1);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskAttemptID);
        PlayerStatsInputFormat.CassandraRecordReader reader = new PlayerStatsInputFormat.CassandraRecordReader();
        reader.initialize(split, context);
        while (reader.nextKeyValue()) {
            System.out.println(String.format("Progress: %.2f%%, loaded: %d/%d", reader.getProgress() * 100, reader.getLoadedItems(), reader.getTotalItems()));
            System.out.println(reader.getToken() + " " + reader.getCurrentValue());
        }
        
        reader.close();
        
        
    }

}
