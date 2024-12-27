package com.minhkakart.bigdata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DecisionTree {
    public Node root;
    
    private int maxDepth = 5;
    private int max_features = 10;
    private int min_samples_split = 100;
    private static final GsonBuilder SERIALIZER = new GsonBuilder().registerTypeAdapter(Node.class, new NodeSerializer());

    public DecisionTree() {
    }

    public DecisionTree(int maxDepth, int max_features, int min_samples_split) {
        this.maxDepth = maxDepth;
        this.max_features = max_features;
        this.min_samples_split = min_samples_split;
    }

    public static class Node {
        public int feature;
        public double threshold;
        public double value;
        public Node left, right;

        public Node(double value) {
            this.value = value;
        }
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

    private Node buildTree(double[][] X, double[] y, int depth) {
//        if (depth >= MAX_DEPTH || y.length < MIN_SAMPLES_SPLIT) {
        if (depth >= maxDepth || y.length < min_samples_split) {
            return new Node(mean(y));
        }

        int bestFeature = -1;
        double bestThreshold = Double.NaN;
        double bestMSE = Double.POSITIVE_INFINITY;

        for (int feature = 0; feature < X[0].length; feature++) {
            double[] thresholds = uniqueValues(X, feature);
            for (double threshold : thresholds) {
                SplitResult split = split(X, y, feature, threshold);
                double mse = calculateMSE(split.leftY, split.rightY);

                if (mse < bestMSE) {
                    bestMSE = mse;
                    bestFeature = feature;
                    bestThreshold = threshold;
                }
            }
        }

        if (bestFeature == -1) {
            return new Node(mean(y));
        }

        SplitResult bestSplit = split(X, y, bestFeature, bestThreshold);
        Node left = buildTree(bestSplit.leftX, bestSplit.leftY, depth + 1);
        Node right = buildTree(bestSplit.rightX, bestSplit.rightY, depth + 1);

        Node node = new Node(Double.NaN);
        node.feature = bestFeature;
        node.threshold = bestThreshold;
        node.left = left;
        node.right = right;

        return node;
    }

    public String toJson() {
        Gson gson = getSerializer();
        return gson.toJson(this.root);
    }

    private static double mean(double[] y) {
        return Arrays.stream(y).average().orElse(0.0);
    }

    private static double[] uniqueValues(double[][] X, int feature) {
        return Arrays.stream(X).mapToDouble(row -> row[feature]).distinct().toArray();
    }

    private static class SplitResult {
        double[][] leftX, rightX;
        double[] leftY, rightY;
    }

    private static SplitResult split(double[][] X, double[] y, int feature, double threshold) {
        List<double[]> leftXList = new ArrayList<>();
        List<Double> leftYList = new ArrayList<>();
        List<double[]> rightXList = new ArrayList<>();
        List<Double> rightYList = new ArrayList<>();

        for (int i = 0; i < X.length; i++) {
            if (X[i][feature] <= threshold) {
                leftXList.add(X[i]);
                leftYList.add(y[i]);
            } else {
                rightXList.add(X[i]);
                rightYList.add(y[i]);
            }
        }

        SplitResult result = new SplitResult();
        result.leftX = leftXList.toArray(new double[0][0]);
        result.rightX = rightXList.toArray(new double[0][0]);
        result.leftY = leftYList.stream().mapToDouble(Double::doubleValue).toArray();
        result.rightY = rightYList.stream().mapToDouble(Double::doubleValue).toArray();
        return result;
    }

    private static double calculateMSE(double[] leftY, double[] rightY) {
        return variance(leftY) * leftY.length + variance(rightY) * rightY.length;
    }

    private static double variance(double[] y) {
        if (y.length == 0) return 0.0;
        double mean = mean(y);
        return Arrays.stream(y).map(v -> (v - mean) * (v - mean)).average().orElse(0.0);
    }
    
    public static Gson getSerializer() {
        return SERIALIZER.create();
    }

}