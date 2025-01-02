package com.minhkakart.bigdata.mapreduce.train;

import com.minhkakart.bigdata.algorithm.DecisionTree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@SuppressWarnings("SpellCheckingInspection")
public class RandomForestTrainReducer extends Reducer<Text, Text, Text, Text> {
    private int max_depth;
    private int max_features;
    private int min_samples_split;
    private int total_samples;
    private final Random random = new Random();

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        // Đọc các tham số cấu hình
        Configuration conf = context.getConfiguration();
        max_depth = Integer.parseInt(conf.get("max_depth", "5"));
        max_features = Integer.parseInt(conf.get("max_features", "5"));
        total_samples = Integer.parseInt(conf.get("total_samples", "100"));
        min_samples_split = Integer.parseInt(conf.get("min_samples_split", "10"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> data = new ArrayList<>();

        // Tập hợp dữ liệu bootstrap
        for (Text value : values) {
            data.add(value.toString());
        }
        Collections.shuffle(data, random);

        // Huấn luyện cây quyết định
        DecisionTree tree = new DecisionTree(max_depth, max_features, min_samples_split);
        tree.fit(data.subList(0, Math.min(Math.max(min_samples_split, random.nextInt(total_samples)), total_samples)));

        // Lưu cây dưới dạng chuỗi JSON
        String treeModel = tree.toJson();
        context.write(key, new Text(treeModel));
    }
}
