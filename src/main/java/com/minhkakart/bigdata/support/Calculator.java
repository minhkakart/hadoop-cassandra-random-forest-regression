package com.minhkakart.bigdata.support;

import java.util.Arrays;

@SuppressWarnings({"SpellCheckingInspection", "unused"})
public class Calculator {

	/**
	 * Tính trung vị của mảng
	 */
	public static double median(double[] arr) {
		Arrays.sort(arr);
		int n = arr.length;
		if (n % 2 == 0) {
			return (arr[n / 2 - 1] + arr[n / 2]) / 2;
		} else {
			return arr[n / 2];
		}
	}

	/**
	 * Tính trung bình cộng của mảng
	 *
	 * @param arr Mảng đầu vào
	 * @return Trung bình cộng
	 */
	public static double mean(double[] arr) {
		return Arrays.stream(arr).average().orElse(0.0);
	}

	/**
	 * Tính độ lệch chuẩn giữa giá trị thực và giá trị dự đoán
	 *
	 * @param actual    Giá trị thực
	 * @param predicted Giá trị dự đoán
	 * @return Độ lệch chuẩn
	 */
	public static double rootMeanSquareError(double[] actual, double[] predicted) {
		double sum = 0;
		for (int i = 0; i < actual.length; i++) {
			sum += Math.pow(actual[i] - predicted[i], 2);
		}
		return Math.sqrt(sum / actual.length);
	}

	/**
	 * Tính độ lỗi trung bình tuyệt đối giữa giá trị thực và giá trị dự đoán
	 *
	 * @param actual    Giá trị thực
	 * @param predicted Giá trị dự đoán
	 * @return Độ lỗi trung bình tuyệt đối
	 */
	public static double meanAbsoluteError(double[] actual, double[] predicted) {
		double sum = 0;
		for (int i = 0; i < actual.length; i++) {
			sum += Math.abs(actual[i] - predicted[i]);
		}
		return sum / actual.length;
	}

	public static double nashSutcliffeEfficiency(double[] actual, double[] predicted) {
		double numerator = 0;
		double denominator = 0;
		double meanObserved = mean(actual);
		for (int i = 0; i < actual.length; i++) {
			numerator += Math.pow(actual[i] - predicted[i], 2);
			denominator += Math.pow(actual[i] - meanObserved, 2);
		}
		return 1 - (numerator / denominator);
	}

	public static double coefficientOfDetermination(double[] actual, double[] predicted) {
		double meanActual = mean(actual);
		double meanPred = mean(predicted);
		double ssr = 0;
		double sst = 0;
		double t = 0;
		for (int i = 0; i < actual.length; i++) {
			ssr += Math.pow(predicted[i] - meanActual, 2);
			sst += Math.pow(actual[i] - meanActual, 2);
			t += (actual[i] - meanActual) * (predicted[i] - meanActual);
		}

		return Math.pow(t / (Math.sqrt(Math.pow(ssr, 2) * Math.pow(sst, 2))), 2);
	}
}
