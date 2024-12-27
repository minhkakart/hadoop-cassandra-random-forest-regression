package com.minhkakart.bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RandomForestReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> data = new ArrayList<>();

        // Tập hợp dữ liệu bootstrap
        for (Text value : values) {
            data.add(value.toString());
        }

        // Huấn luyện cây quyết định
        DecisionTree tree = new DecisionTree();
        tree.fit(data);

        // Lưu cây dưới dạng chuỗi JSON
        String treeModel = tree.toJson();
        context.write(key, new Text(treeModel));
    }
}
