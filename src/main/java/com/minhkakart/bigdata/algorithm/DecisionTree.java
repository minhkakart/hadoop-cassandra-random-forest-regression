package com.minhkakart.bigdata.algorithm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.minhkakart.bigdata.support.Calculator;
import com.minhkakart.bigdata.support.NodeSerializer;

import java.util.*;

@SuppressWarnings("SpellCheckingInspection")
public class DecisionTree {
    private Node root;

    private int maxDepth;
    private int max_features;
    private int min_samples_split;

    private static final GsonBuilder SERIALIZER = new GsonBuilder().registerTypeAdapter(Node.class, new NodeSerializer());

    public DecisionTree() {
    }

    public DecisionTree(int maxDepth, int max_features, int min_samples_split) {
        this.maxDepth = maxDepth;
        this.max_features = max_features;
        this.min_samples_split = min_samples_split;
    }

    public void fit(List<String> data) {
        // Chuyển dữ liệu thành mảng số
        double[][] X = new double[data.size()][];
        double[] y = new double[data.size()];

        for (int i = 0; i < data.size(); i++) {
            String[] parts = data.get(i).split(",");
            X[i] = Arrays.stream(parts, 0, parts.length - 1).mapToDouble(Double::parseDouble).toArray();
            y[i] = Double.parseDouble(parts[parts.length - 1]);
        }

        // Xây dựng cây
        this.root = buildTree(X, y, 0);
    }

    /**
     * Xây dựng cây quyết định từ tập dữ liệu
     *
     * @param X     Ma trận dữ liệu
     * @param y     Nhãn
     * @param depth Độ sâu của cây
     * @return Node gốc của cây
     */
    private Node buildTree(double[][] X, double[] y, int depth) {
        if (depth >= maxDepth || y.length < min_samples_split) {
            return new Node(Calculator.mean(y));
        }

        int bestFeature = -1; // Chỉ số của đặc trưng tốt nhất
        double bestThreshold = Double.NaN; // Ngưỡng tốt nhất (giá trị của 1 đặc trưng trong tập dữ liệu)
        double bestMSE = Double.POSITIVE_INFINITY; // Càng nhỏ càng tốt
        SplitResult bestSplit = null;

        // Chọn ngẫu nhiên một số lượng đặc trưng
        int[] featureSubset = selectRandomFeatures(X[0].length, max_features);

        // Tìm đặc trưng tốt nhất
        for (int feature : featureSubset) {
            double[] thresholds = uniqueValues(X, feature);
            for (double threshold : thresholds) {
                SplitResult split = new SplitResult(X, y, feature, threshold);
                double mse = split.getScore();

                if (mse < bestMSE) {
                    bestMSE = mse;
                    bestFeature = feature;
                    bestThreshold = threshold;
                    bestSplit = split;
                }
            }
        }

        if (bestFeature == -1) {
            return new Node(Calculator.mean(y));
        }

        Node left = buildTree(bestSplit.getLeftX(), bestSplit.getLeftY(), depth + 1);
        Node right = buildTree(bestSplit.getRightX(), bestSplit.getRightY(), depth + 1);

        return new Node(bestFeature, bestThreshold, left, right);
    }

    private int[] selectRandomFeatures(int totalFeatures, int maxFeatures) {
        Random random = new Random();
        List<Integer> allFeatures = new ArrayList<>();
        for (int i = 0; i < totalFeatures; i++) {
            allFeatures.add(i);
        }
        Collections.shuffle(allFeatures, random);
        return allFeatures.subList(0, Math.min(maxFeatures, totalFeatures)).stream().mapToInt(i -> i).toArray();
    }

    /**
     * Dự đoán giá trị của một mẫu dữ liệu
     *
     * @param sample Mẫu dữ liệu
     * @return Giá trị dự đoán
     */
    @SuppressWarnings("unused")
    public double predict(double[] sample) {
        return traverseTree(root, sample);
    }

    /**
     * Duyệt cây để dự đoán giá trị
     *
     * @param node   Nút hiện tại
     * @param sample Mẫu dữ liệu
     * @return Giá trị dự đoán
     */
    private double traverseTree(Node node, double[] sample) {
        if (node.getLeft() == null && node.getRight() == null) {
            // Đây là một node lá, trả về giá trị trung bình của nhãn
            return node.getValue();
        }

        // So sánh giá trị của đặc trưng tại nút hiện tại với ngưỡng
        if (sample[node.getFeature()] <= node.getThreshold()) {
            if (node.getLeft() == null) {
                return node.getValue();
            }
            return traverseTree(node.getLeft(), sample);
        } else {
            if (node.getRight() == null) {
                return node.getValue();
            }
            return traverseTree(node.getRight(), sample);
        }
    }


    /**
     * Tìm các giá trị không trùng lặp của một đặc trưng
     *
     * @param X            Ma trận dữ liệu
     * @param featureIndex Chỉ số của đặc trưng
     * @return Mảng các giá trị không trùng lặp
     */
    private static double[] uniqueValues(double[][] X, int featureIndex) {
        return Arrays.stream(X).mapToDouble(row -> row[featureIndex]).distinct().toArray();
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public static Gson getSerializer() {
        return SERIALIZER.create();
    }

    public String toJson() {
        Gson gson = getSerializer();
        return gson.toJson(this.root);
    }

}