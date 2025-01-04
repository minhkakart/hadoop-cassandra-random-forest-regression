package com.minhkakart.bigdata.mapreduce.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RandomForestTestReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        double count = 0;
        while (values.iterator().hasNext()){
            sum += values.iterator().next().get();
            count++;
        }
        context.write(key,new DoubleWritable(sum/count));
    }
}
