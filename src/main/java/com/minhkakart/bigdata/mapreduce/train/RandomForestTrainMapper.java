package com.minhkakart.bigdata.mapreduce.train;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@SuppressWarnings("SpellCheckingInspection")
public class RandomForestTrainMapper extends Mapper<Text, Text, Text, Text> {
	private int numTrees; // Số lượng cây trong rừng
	private int maxFeatures; // Số lượng đặc trưng tối đa

	@Override
	protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		super.setup(context);

		// Đọc tham số cấu hình
		numTrees = Integer.parseInt(context.getConfiguration().get("n_estimators", "10"));
		maxFeatures = Integer.parseInt(context.getConfiguration().get("max_features", "5"));
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// Phân tích dòng dữ liệu
		String line = value.toString();
		String[] parts = line.split(",");
		if (parts.length < maxFeatures) return; // Bỏ qua nếu dữ liệu không đủ

		// Phát ngẫu nhiên mẫu này cho các cây
		for (int i = 0; i < numTrees; i++) {
			String treeKey = "Tree_" + i;
			context.write(new Text(treeKey), new Text(line));
		}
	}
}
