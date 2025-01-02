package com.minhkakart.bigdata.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Phân chia tập dữ liệu thành 2 phần dựa trên một ngưỡng
 */
@SuppressWarnings({"SpellCheckingInspection", "unused"})
public class SplitResult {
	/**
	 * Dữ liệu bên nhỏ hơn hoặc bằng ngưỡng
	 */
	private final double[][] leftX;
	/**
	 * Dữ liệu bên lớn hơn ngưỡng
	 */
	private final double[][] rightX;
	/**
	 * Nhãn của dữ liệu bên nhỏ hơn hoặc bằng ngưỡng
	 */
	private final double[] leftY;
	/**
	 * Nhãn của dữ liệu bên lớn hơn ngưỡng
	 */
	private final double[] rightY;

	/**
	 * Mean Squared Error (MSE) của phân chia
	 */
	private final double score;

	public SplitResult(double[][] X, double[] y, int feature, double threshold) {
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

		this.leftX = leftXList.toArray(new double[0][0]);
		this.rightX = rightXList.toArray(new double[0][0]);
		this.leftY = leftYList.stream().mapToDouble(Double::doubleValue).toArray();
		this.rightY = rightYList.stream().mapToDouble(Double::doubleValue).toArray();

		this.score = calculateMAE();
	}

	/**
	 * Tính toán Mean Absolute Error (MAE) của một phân chia
	 *
	 * @return MAE
	 */
	private double calculateMAE() {
		return Arrays.stream(leftY).map(y -> Math.abs(y - Arrays.stream(leftY).average().orElse(0.0))).sum() +
			   Arrays.stream(rightY).map(y -> Math.abs(y - Arrays.stream(rightY).average().orElse(0.0))).sum();
	}

	/**
	 * Tính toán Mean Squared Error (MSE) của một phân chia
	 */
	private double calculateMSE() {
		return variance(leftY) * leftY.length + variance(rightY) * rightY.length;
	}

	/**
	 * Tính toán phương sai
	 */
	private double variance(double[] y) {
		if (y.length == 0) return 0.0;
		double mean = Arrays.stream(y).average().orElse(0.0);
		return Arrays.stream(y).map(v -> (v - mean) * (v - mean)).average().orElse(0.0);
	}

	public double[][] getLeftX() {
		return leftX;
	}

	public double[][] getRightX() {
		return rightX;
	}

	public double[] getLeftY() {
		return leftY;
	}

	public double[] getRightY() {
		return rightY;
	}

	public double getScore() {
		return score;
	}
}