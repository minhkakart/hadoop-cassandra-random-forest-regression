package com.minhkakart.bigdata.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.net.InetSocketAddress;
import java.util.UUID;

public class TestTreesRecordWriter extends RecordWriter<Text, DoubleWritable> {
    CqlSession session;
    int sessionId;
    String contactPoint;
    String datacenter;
    String keyspace;
    String trainTable;
    String kqTable;

    public TestTreesRecordWriter(Configuration conf) {
        // Read configuration
        contactPoint = conf.get("cassandra.contact.point", "localhost");
        datacenter = conf.get("cassandra.datacenter", "datacenter1");
        keyspace = conf.get("cassandra.keyspace");
        trainTable = conf.get("cassandra.output.columnfamily");
        kqTable = conf.get("cassandra.predicted.columnfamily");

        if (keyspace == null || trainTable == null || kqTable == null) {
            throw new IllegalArgumentException("Cassandra input configuration missing keyspace or outputTable.");
        }

        // Connect to Cassandra
        session = new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress(contactPoint, 9042))
                .withLocalDatacenter(datacenter)
                .withKeyspace(keyspace)
                .build();
        
        // Reachieved last session id
        String lastSessionIdCql = "SELECT max(session) FROM " + keyspace + "." + trainTable;
        PreparedStatement preparedLastSession = session.prepare(lastSessionIdCql);
        ResultSet resultSet = session.execute(preparedLastSession.bind());
        Row lastSessionRow = resultSet.one();
        assert lastSessionRow != null;
        sessionId = Integer.parseInt(conf.get("cassandra.output.session", String.valueOf(lastSessionRow.getInt(0))));

    }

    @Override
    public void write(Text text, DoubleWritable doubleWritable) {
        // Prepare CQL statement
        String insertStatement = "INSERT INTO " + keyspace + "." + kqTable + " (id, session, record_id, predicted_value) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStatement = session.prepare(insertStatement);
        session.execute(preparedStatement.bind(UUID.randomUUID(), sessionId, UUID.fromString(text.toString()), doubleWritable.get()));

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
        if (session != null) {
            session.close();
        }
    }
}
