package com.minhkakart.bigdata.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    
    @SuppressWarnings("DuplicatedCode")
    public static class PlayerStatRecordWriter extends RecordWriter<Text, Text> {
        private final CqlSession session;
        private final PreparedStatement preparedStatement;
        private final ConcurrentLinkedQueue<BoundStatement> statementQueue;
        private final int sessionId;

        public PlayerStatRecordWriter(Configuration conf) {
            // Read configuration
            String contactPoint = conf.get("cassandra.contact.point", "localhost");
            String datacenter = conf.get("cassandra.datacenter", "datacenter1");
            String keyspace = conf.get("cassandra.keyspace");
            String outputTable = conf.get("cassandra.output.columnfamily");

            if (keyspace == null || outputTable == null) {
                throw new IllegalArgumentException("Cassandra input configuration missing keyspace or outputTable.");
            }

            // Connect to Cassandra
            session = new CqlSessionBuilder()
                    .addContactPoint(new InetSocketAddress(contactPoint, 9042))
                    .withLocalDatacenter(datacenter)
                    .withKeyspace(keyspace)
                    .build();
            
            // Reachieved last session id
            String lastSessionIdCql = "SELECT max(session) FROM " + keyspace + "." + outputTable;
            PreparedStatement preparedLastSession = session.prepare(lastSessionIdCql);
            ResultSet resultSet = session.execute(preparedLastSession.bind());
            Row lastSessionRow = resultSet.one();
            assert lastSessionRow != null;
            if (lastSessionRow.isNull(0)) {
                sessionId = 1;
            } else {
                sessionId = lastSessionRow.getInt(0) + 1;
            }

            // Prepare CQL statement
            String insertStatement = "INSERT INTO " + keyspace + "." + outputTable + " (id, session, tree_name, value) VALUES (?, ?, ?, ?)";
            preparedStatement = session.prepare(insertStatement);

            // Queue for batch processing
            statementQueue = new ConcurrentLinkedQueue<>();
        }

        @Override
        public void write(Text key, Text value) throws IOException {
            try {
                // Bind parameters to the prepared statement
                BoundStatement boundStatement = preparedStatement.bind(UUID.randomUUID(), sessionId, key.toString(), value.toString());
                statementQueue.add(boundStatement);

                // Execute the statement in batches
                if (statementQueue.size() >= 50) {
                    flushBatch();
                }
            } catch (Exception e) {
                throw new IOException("Error writing to Cassandra", e);
            }
        }

        private void flushBatch() {
            BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.LOGGED);
            while (!statementQueue.isEmpty()) {
                batchBuilder.addStatement(statementQueue.poll());
            }
            CompletionStage<AsyncResultSet> resultStage = session.executeAsync(batchBuilder.build());
            resultStage.toCompletableFuture().join();
        }

        @Override
        public void close(TaskAttemptContext context) {
            // Flush remaining statements
            flushBatch();
            if (session != null) session.close();
        }
    }
    
    public static class PlayerStatOutputCommitter extends OutputCommitter {
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
}
