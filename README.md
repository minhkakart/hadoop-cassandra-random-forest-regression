# Big Data Project

This project is designed to train a Random Forest model using data stored in a Cassandra database. The project uses
Hadoop for distributed processing and integrates with Cassandra for data storage and retrieval.

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

   Ensure that your Cassandra instance is running and accessible. Update the configuration parameters if needed.

    1. **Prepare the input data:**

       Insert the input data into the Cassandra database. The input data should be stored in the `player_stats` column
       family with the following schema:

       | id  | age | height_cm | weight_kg | potential | international_reputation | weak_foot | shooting | passing | dribbling | defending | physic | value_eur |
                                                |-----|-----|-----------|-----------|-----------|--------------------------|-----------|----------|---------|-----------|-----------|--------|-----------|
       ```
         CREATE TABLE IF NOT EXISTS bigdata.player_stats (
             id UUID PRIMARY KEY,
             age INT,
             height_cm INT,
             weight_kg INT,
             potential INT,
             international_reputation INT,
             weak_foot INT,
             shooting INT,
             passing INT,
             dribbling INT,
             defending INT,
             physic INT,
             value_eur INT
         );
         ```
    2. **Prepare the output data:**

       Create an empty column family `trained_trees` to store the trained Random Forest model.

        ```
          CREATE TABLE IF NOT EXISTS bigdata.trained_trees (
                id UUID PRIMARY KEY,
                session int,
                tree_name text,
                tree text
          );
        ```
    3. **Prepare the predict table:**
       Create an empty column family `predicted` to store the predicted values.

        ```
          CREATE TABLE IF NOT EXISTS bigdata.predicted (
                id UUID PRIMARY KEY,
                session int,
                record_id UUID,
                value_eur int
          );
        ```

## Configuration

Update the configuration file with the following parameters:

- `n_estimators`: Number of trees in the Random Forest
- `max_depth`: Maximum depth of the trees
- `max_features`: Maximum number of features considered for splitting
- `min_samples_split`: Minimum number of samples required to split an internal node
- `cassandra.contact.point`: Cassandra contact point (e.g., `localhost`)
- `cassandra.datacenter`: Cassandra datacenter (default: `datacenter1`)
- `cassandra.keyspace`: Keyspace in Cassandra containing the input and output tables
- `cassandra.input.columnfamily`: Input table in Cassandra containing the training data
- `cassandra.output.columnfamily`: Output table in Cassandra to store the trained Random Forest model
- `cassandra.predicted.columnfamily`: Output table for predicted values in Cassandra to store the predicted values

## Running the Job

1. **Submit the Hadoop job:**

   ```sh
   hadoop jar target/BigdataBtl-RandomForest.jar com.minhkakart.bigdata.TrainCassandraJob -D n_estimators=100 -D max_depth=10 -D max_features=5 -D min_samples_split=2 -D cassandra.contact.point=localhost -D cassandra.keyspace=bigdata -D cassandra.input.columnfamily=player_stats -D cassandra.output.columnfamily=trained_trees -D cassandra.predicted.columnfamily=predicted
   ```

2. **Monitor the job:**

   You can monitor the job progress through the Hadoop web interface.

## Code Structure

- `com.minhkakart.bigdata.algorithm`: Contains the Random Forest implementation.
    - `DecisionTree`: Represents a decision tree.
- `com.minhkakart.bigdata.cassandra`: Contains the Cassandra utilities.
    - `PlayerStats`: Represents the input data schema.
    - `TrainedTree`: Represents the output data schema.
    - `PlayerStatsRecordReader`: Reads the input data from Cassandra.
    - `TrainedTreeRecordWriter`: Writes the output data to Cassandra.
    - `TestTreesRecordWriter`: Writes the predicted values to Cassandra.
- `com.minhkakart.bigdata.mapreduce`: Contains the Hadoop map-reduce implementation.
    - `train.RandomForestTrainMapper`: Mapper class for training the Random Forest model.
    - `train.RandomForestTrainReducer`: Reducer class for training the Random Forest model.
    - `test.RandomForestTestMapper`: Mapper class for testing the Random Forest model.
    - `test.RandomForestTestReducer`: Reducer class for testing
    -  `score.ScoreMapper`: Mapper class for scoring
    - `score.ScoreReducer`: Reducer class for scoring
- `com.minhkakart.bigdata`: Contains the main job class.
    - `TrainCassandraJob`: Main job class for training the Random Forest model.
    - `TestCassandraJob`: Main job class for testing the Random Forest model.
    - `ScoreJob`: Main job class for scoring the Random Forest model.
    -  `AllJob`: Main job class for training, testing and scoring the Random Forest model.

## License