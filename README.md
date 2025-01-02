# Big Data Project

This project is designed to train a Random Forest model using data stored in a Cassandra database. The project uses Hadoop for distributed processing and integrates with Cassandra for data storage and retrieval.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Apache Hadoop
- Apache Cassandra

## Setup

1. **Clone the repository:**

   ```sh
   git clone https://github.com/minhkakart/hadoop-cassandra-random-forest-regression.git
   cd hadoop-cassandra-random-forest-regression
   ```

2. **Build the project:**

   ```sh
   mvn clean package
   ```

3. **Configure Cassandra:**

   Ensure that your Cassandra instance is running and accessible. Update the configuration parameters in the `TrainCassandraJob` and `PlayerStatRecordReader` classes as needed.

## Configuration

Update the `hadoop` configuration file with the following parameters:

- `n_estimators`: Number of trees in the Random Forest
- `max_depth`: Maximum depth of the trees
- `max_features`: Maximum number of features considered for splitting
- `min_samples_split`: Minimum number of samples required to split an internal node
- `cassandra.contact.point`: Cassandra contact point (e.g., `localhost`)
- `cassandra.keyspace`: Keyspace in Cassandra
- `cassandra.input.columnfamily`: Input column family in Cassandra
- `cassandra.datacenter`: Cassandra datacenter (default: `datacenter1`)

## Running the Job

1. **Submit the Hadoop job:**

   ```sh
   hadoop jar target/bigdata-1.0-SNAPSHOT.jar com.minhkakart.bigdata.TrainCassandraJob -D n_estimators=100 -D max_depth=10 -D max_features=5 -D min_samples_split=2 -D cassandra.contact.point=localhost -D cassandra.keyspace=bigdata -D cassandra.input.columnfamily=player_stats
   ```

2. **Monitor the job:**

   You can monitor the job progress through the Hadoop web interface.

## Code Structure

- `TrainCassandraJob.java`: Main class to configure and run the Hadoop job.
- `PlayerStatRecordReader.java`: Custom `RecordReader` to read data from Cassandra.
- `PlayerStat`
- `RandomForestTrainMapper.java`: Mapper class for the Random Forest training.
- `RandomForestTrainReducer.java`: Reducer class for the Random Forest training.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.