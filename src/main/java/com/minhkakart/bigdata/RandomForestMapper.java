package com.minhkakart.bigdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class RandomForestMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Random random = new Random();
    private final int numTrees = random.nextInt(100); // Số lượng cây trong rừng

    
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Phân tích dòng dữ liệu
        String line = value.toString();
        String[] parts = line.split(",");
        if (parts.length < 2) return; // Bỏ qua nếu dữ liệu không đủ

        // Phát ngẫu nhiên mẫu này cho các cây
        for (int i = 0; i < numTrees; i++) {
            String treeKey = "Tree" + i;
            context.write(new Text(treeKey), new Text(line));
        }
    }
}
