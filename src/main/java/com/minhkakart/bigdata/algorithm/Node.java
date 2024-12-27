package com.minhkakart.bigdata.algorithm;

public class Node {
    private final int feature;
    private final double threshold;
    private final double value;
    private final Node left;
    private final Node right;

    public Node(double value) {
        this.value = value;
        this.feature = -1;
        this.threshold = Double.NaN;
        this.left = null;
        this.right = null;
    }

    public Node(int feature, double threshold, Node left, Node right) {
        this.feature = feature;
        this.threshold = threshold;
        this.left = left;
        this.right = right;
        this.value = Double.NaN;
    }

    public int getFeature() {
        return feature;
    }

    public double getThreshold() {
        return threshold;
    }

    public double getValue() {
        return value;
    }

    public Node getLeft() {
        return left;
    }

    public Node getRight() {
        return right;
    }
}
