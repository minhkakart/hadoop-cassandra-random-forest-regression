import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;

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


        /*PlayerStatInputFormat.PlayerStatsSplit split = new PlayerStatInputFormat.PlayerStatsSplit();
        Configuration conf = new Configuration();
        conf.set("cassandra.contact.point", "nodemaster");
        conf.set("cassandra.input.keyspace", "bigdata");
        conf.set("cassandra.input.datacenter", "datacenter1");
        TaskAttemptID taskAttemptID = new TaskAttemptID("job", 1, true, 1, 1);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskAttemptID);
        PlayerStatInputFormat.CassandraRecordReader reader = new PlayerStatInputFormat.CassandraRecordReader();
        reader.initialize(split, context);
        while (reader.nextKeyValue()) {
            System.out.println(String.format("Progress: %.2f%%, loaded: %d/%d", reader.getProgress() * 100, reader.getLoadedItems(), reader.getTotalItems()));
            System.out.println(reader.getToken() + " " + reader.getCurrentValue());
        }
        
        reader.close();*/

        /*// Connect to Cassandra
        String contactPoint = "nodemaster";
        String datacenter = "datacenter1";
        String keyspace = "bigdata";
        String table = "trained_trees";
        CqlSession session = new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress(contactPoint, 9042))
                .withLocalDatacenter(datacenter)
                .withKeyspace(keyspace)
                .build();

        // Prepare CQL statement
        String cql = "INSERT INTO " + keyspace + "." + table + " (id, session, value) VALUES (?, ?, ?)";
        PreparedStatement insertStatement = session.prepare(cql);

        // Execute the statement in batches
        BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.LOGGED);
        for (int i = 0; i < 9; i++) {
            BoundStatement boundStatement = insertStatement.bind(UUID.randomUUID(), 1, "test_" + i);
            batchBuilder.addStatement(boundStatement);
        }
        CompletionStage resultSet = session.executeAsync(batchBuilder.build());
        resultSet.toCompletableFuture().join();

//        session.execute(preparedStatement.bind(UUID.randomUUID(), 1, "test"));

        session.close();

        System.out.println("Done");*/

		String contactPoint = "nodemaster";
		String datacenter = "datacenter1";
		String keyspace = "bigdata";
		CqlSession session = new CqlSessionBuilder()
			   .addContactPoint(new InetSocketAddress(contactPoint, 9042))
			   .withLocalDatacenter(datacenter)
			   .withKeyspace(keyspace)
			   .build();

		String cql = "SELECT max(session) FROM trained_trees";
		PreparedStatement preparedStatement = session.prepare(cql);
		ResultSet resultSet = session.execute(preparedStatement.bind());
		Row row = resultSet.one();
		System.out.println(row.isNull(0));
		System.out.println(row.getInt(0));

		session.close();

	}

}
