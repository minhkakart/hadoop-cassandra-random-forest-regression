package com.minhkakart.bigdata.mapreduce.score;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<NullWritable, DoubleWritable, Text, Text> {

	@Override
	protected void reduce(NullWritable key, Iterable<DoubleWritable> values,
			Reducer<NullWritable, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		double sum = 0;
		int count = 0;
		while (values.iterator().hasNext()) {
			sum += values.iterator().next().get();
			count++;
		}
		
		System.out.println(sum/count);
	}
	
}
