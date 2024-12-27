package com.minhkakart.bigdata.support;

import java.util.Arrays;

@SuppressWarnings("SpellCheckingInspection")
public class Calculator {
    /**
     * Tính trung bình cộng của mảng
     *
     * @param arr Mảng đầu vào
     * @return Trung bình cộng
     */
    public static double mean(double[] arr) {
        return Arrays.stream(arr).average().orElse(0.0);
    }
}
