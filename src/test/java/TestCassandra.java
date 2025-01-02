import com.minhkakart.bigdata.algorithm.DecisionTree;
import com.minhkakart.bigdata.algorithm.Node;
import com.minhkakart.bigdata.support.Calculator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class TestCassandra {
	public static void main(String[] args) throws FileNotFoundException {

		List<DecisionTree> forest = new ArrayList<>(100);
		String forestPath = "E:/TLU_Subject/Ki_7/BigData/output/btl-test-cassandra/part-r-00000";
		BufferedReader reader = new BufferedReader(new FileReader(forestPath));
		reader.lines().forEach(line -> {
			String[] split = line.split("\t");
			DecisionTree tree = new DecisionTree();
			tree.setRoot(DecisionTree.getSerializer().fromJson(split[1], Node.class));
			forest.add(tree);
		});
		double[][] testData = new double[][]{
			   new double[]{33, 187, 83, 94, 5, 4, 93, 81, 89, 35, 79, 77000000},
			   new double[]{31, 170, 72, 94, 5, 4, 91, 88, 96, 32, 61, 110500000},
			   new double[]{26, 175, 68, 93, 5, 5, 84, 83, 95, 32, 59, 118500000},
			   new double[]{27, 181, 70, 92, 4, 5, 86, 92, 87, 60, 78, 102000000},
			   new double[]{32, 184, 82, 91, 4, 3, 63, 71, 71, 91, 84, 51000000},
			   new double[]{31, 182, 86, 91, 5, 4, 90, 79, 88, 52, 85, 80000000},
			   new double[]{32, 172, 66, 91, 4, 4, 76, 90, 91, 70, 67, 67000000},
			   new double[]{27, 173, 74, 91, 4, 4, 82, 86, 94, 35, 67, 93000000},
			   new double[]{32, 187, 78, 90, 3, 3, 48, 65, 62, 89, 84, 44000000},
			   new double[]{28, 183, 76, 90, 4, 5, 82, 89, 82, 74, 69, 76500000},
			   new double[]{29, 184, 80, 90, 4, 4, 89, 75, 85, 41, 82, 77000000},
		};


		double[] actual = new double[testData.length];
		double[] predictedArr = new double[testData.length];

		for (int i = 0; i < testData.length; i++) {
			actual[i] = testData[i][testData[0].length - 1];
			double[] predicted = new double[forest.size()];
			for (int j = 0; j < forest.size(); j++) {
				predicted[j] = forest.get(j).predict(testData[i]);
			}

			double mean = Calculator.mean(predicted);
			double median = Calculator.median(predicted);

			predictedArr[i] = (mean + median) / 2;
		}

		for (double predicted : predictedArr) {
			System.out.println((int) predicted);
		}

		System.out.println("Root mean square error: " + Calculator.rootMeanSquareError(actual, predictedArr));
		System.out.println("Mean absolute error: " + Calculator.meanAbsoluteError(actual, predictedArr));
		System.out.println("Nash-Sutcliffe efficiency: " + Calculator.nashSutcliffeEfficiency(actual, predictedArr));
		System.out.println("Coefficient of determination: " + Calculator.coefficientOfDetermination(actual, predictedArr));

	}
}
