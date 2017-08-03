package org.sparkexample.classifiers1;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.classification.KNNClassificationModel;
import org.apache.spark.ml.classification.KNNClassifier;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sparkexample.pojo1.PojoRow;

import scala.Tuple2;

public class CustomNaiveBayes {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("org.sparkexample.WordCount2").setMaster("local[*]");
		/*
		 * @SuppressWarnings("resource") JavaSparkContext sc = new
		 * JavaSparkContext(sparkConf);
		 */
		SparkContext sc1 = new SparkContext(sparkConf);
		@SuppressWarnings("resource")
		SparkSession sqlSpark = new SparkSession(sc1);

		// Load and parse the data file.
		
		DataExtraction data = new DataExtraction(sc1);

		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<PojoRow>[] splits = data.getDatalabeledPoint().randomSplit(new double[] { 0.7, 0.1, 0.2 });

		JavaRDD<PojoRow> trainingData = splits[0];
		JavaRDD<PojoRow> testData = splits[1]; // arxiki morfi , meta allaxtikan
												// apo ton knn me
												// traindataCollectedwithLabelFromKNNFromTestData
												// san lista

		JavaRDD<PojoRow> testDataForValidation = splits[2];

		
		final InitialParameters baseModel = new InitialParameters(trainingData);
		
		// --------------end of sums ---------------and probabilities
		// calculating accuracy of Naive old model in testDataForValidation
		JavaPairRDD<Double, Double> predictionAndLabelForTestDataForValidationwithOldModel = testData
				.mapToPair(new PairFunction<PojoRow, Double, Double>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Double> call(PojoRow p) {
						return new Tuple2<Double, Double>(NafiBayesModel.classify(
								NafiBayesModel.calculatePxgivenC(baseModel.possibilityOfXEq1givenCgood, baseModel.possibilityOfX흎0givenCbad, p.features.toArray()),
								NafiBayesModel.calculatePxgivenC(baseModel.possibilityOfXEq1givenCgood, baseModel.possibilityOfX흎0givenCbad, p.features.toArray()),
								baseModel.posCgood,
								baseModel.posCbad),p.label);
					}
				});
		double acccuracyOnTestDataWithOldModel = predictionAndLabelForTestDataForValidationwithOldModel
				.filter(new Function<Tuple2<Double, Double>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<Double, Double> pl) {
						return pl._1().equals(pl._2());
					}
				}).count() / (double) testData.count();
		
		// Creating a new rdd of predictions of Naive old model and feautres in
		// order to calculate Entropy of it

	/*	JavaRDD<PojoRow> testDataForValidationWithOldModelPredicted = testData
				.map(new Function<PojoRow, PojoRow>() {

					public PojoRow call(PojoRow v1) throws Exception {
						// TODO Auto-generated method stub
						return new PojoRow(NafiBayesModel.classify(v1.getfeatures().toArray(),
								probabilityCequalsGoodtweet, probabilityCequalsBadTweet), v1.features);
					}
				});
		if (doesModelNeedRetrain(trainingData, testDataForValidationWithOldModelPredicted)) {

			// ===============================================================================================================================================================
			
			// AMMEND test set to model
			// calculating the count of combined training model
			double newCountOfTrainingData = trainingData.count()
					+ traindataCollectedwithLabelFromKNNFromTestData.size();
			// testData3 is the dataset for ammending
			// Double ammentedProbabilityCequalsGoodtweet = testData3.

			// classification me ammending
			double probabilityCequalsGoodtweetAmmendedbytestDatafromKNN = probabilityCequalsGoodtweet;
			double probabilityCequalsBadtweetAmmendedbytestDatafromKNN = probabilityCequalsBadTweet;

			// lack of parametizing global variable through nodes have to get
			// dataset to master in order to
			// modify the p(c) and p(x|c) accordingly
			double m = (double) goodTweetMapNumber + 2;
			double m2 = (double) (trainingDataCount - goodTweetMapNumber) + 2;

			for (PojoRow pj : traindataCollectedwithLabelFromKNNFromTestData) {

				if (pj.label == 1.0) {
					NafiBayesModel.possibilityOfXEq1givenCgood.clear();

					probabilityCequalsGoodtweetAmmendedbytestDatafromKNN = (((double) newCountOfTrainingData
							/ ((double) newCountOfTrainingData + 1))
							* probabilityCequalsGoodtweetAmmendedbytestDatafromKNN)
							+ (double) (1 / (newCountOfTrainingData + 1));

					probabilityCequalsBadtweetAmmendedbytestDatafromKNN = (double) ((double) newCountOfTrainingData
							/ ((double) newCountOfTrainingData + 1))
							* (double) probabilityCequalsBadtweetAmmendedbytestDatafromKNN;
					for (int i = 0; i < pj.features.toArray().length; i++) {
						if (pj.features.toArray()[i] == 1.00) {
							pojoOfProbabilitiesOfGoodTweetsForAeq1.features
									.toArray()[i] = (double) pojoOfProbabilitiesOfGoodTweetsForAeq1.features.toArray()[i]
											* (m / (m + 1)) + (1 / (1 + m));
							NafiBayesModel.possibilityOfXEq1givenCgood
									.add((double) pojoOfProbabilitiesOfGoodTweetsForAeq1.features.toArray()[i]);
						} else {
							pojoOfProbabilitiesOfGoodTweetsForAeq1.features
									.toArray()[i] = (double) pojoOfProbabilitiesOfGoodTweetsForAeq1.features.toArray()[i]
											* (m / (m + 1));
							NafiBayesModel.possibilityOfXEq1givenCgood
									.add((double) pojoOfProbabilitiesOfGoodTweetsForAeq1.features.toArray()[i]);
						}
					}

				} else {
					NafiBayesModel.possibilityOfX흎1givenCbad.clear();

					probabilityCequalsBadtweetAmmendedbytestDatafromKNN = (double) (newCountOfTrainingData
							/ ((double) newCountOfTrainingData + 1))
							* probabilityCequalsBadtweetAmmendedbytestDatafromKNN
							+ (double) (1 / ((double) newCountOfTrainingData + 1));
					probabilityCequalsGoodtweetAmmendedbytestDatafromKNN = (double) ((double) newCountOfTrainingData
							/ ((double) newCountOfTrainingData + 1))
							* probabilityCequalsGoodtweetAmmendedbytestDatafromKNN;
					for (int i = 0; i < pj.features.toArray().length; i++) {
						if (pj.features.toArray()[i] == 1.00) {
							pojoOfProbabilitiesOfBadTweets.features
									.toArray()[i] = (double) pojoOfProbabilitiesOfBadTweets.features.toArray()[i]
											* (m2 / (m2 + 1)) + (1 / (1 + m2));
							NafiBayesModel.possibilityOfX흎1givenCbad
									.add((double) pojoOfProbabilitiesOfBadTweets.features.toArray()[i]);
						} else {
							pojoOfProbabilitiesOfBadTweets.features
									.toArray()[i] = (double) pojoOfProbabilitiesOfBadTweets.features.toArray()[i]
											* (m2 / (m2 + 1));
							NafiBayesModel.possibilityOfX흎1givenCbad
									.add((double) pojoOfProbabilitiesOfBadTweets.features.toArray()[i]);

						}
					}

					// end of ammend

				}
			}
			final double probabilityCequalsGoodtweetAmmendedbytestDatafromKNNfinal = probabilityCequalsGoodtweetAmmendedbytestDatafromKNN;
			final double probabilityCequalsBadtweetAmmendedbytestDatafromKNNfinal = probabilityCequalsBadtweetAmmendedbytestDatafromKNN;

			JavaPairRDD<Double, Double> predictionAndLabelForTestDataforValidationwithNewModel = testData
					.mapToPair(new PairFunction<PojoRow, Double, Double>() {

						private static final long serialVersionUID = 1L;

						public Tuple2<Double, Double> call(PojoRow p) {
							return new Tuple2<Double, Double>(
									NafiBayesModel.classify(p.getfeatures().toArray(),
											probabilityCequalsGoodtweetAmmendedbytestDatafromKNNfinal,
											probabilityCequalsBadtweetAmmendedbytestDatafromKNNfinal),
									p.getLabel());
						}
					});
			double accuracyonTestDataForValidationwithNewModel = predictionAndLabelForTestDataforValidationwithNewModel
					.filter(new Function<Tuple2<Double, Double>, Boolean>() {

						private static final long serialVersionUID = 1L;

						public Boolean call(Tuple2<Double, Double> pl) {
							return pl._1().equals(pl._2());
						}
					}).count() / (double) testData.count();

		*/	System.out.println("The accuracy of Old naive model with dataForValidation is "
					+ acccuracyOnTestDataWithOldModel);
			

			// end of ammend
		}
	

	

	


	public static boolean doesModelNeedRetrain(JavaRDD<PojoRow> trainingData, JavaRDD<PojoRow> testData) {
		double entropyOfTestDataSet = getEntropyOfADataSet(testData);
		double entropyOfTrainDataset = getEntropyOfADataSet(trainingData);

		
		// based on the assumption of the training dataset is chosed to be
		// balanced about positive and negative tweets
		// which means the entropy of it is close to 1
		/*if ((entropyOfTrainDataset - entropyOfTestDataSet) > 0.1) {
			return true;
		}
		return false;*/
		// dummy logic in order to continue with ammending
		if ((entropyOfTrainDataset != entropyOfTestDataSet)) {
			return true;
		}
		return false;
	}

	public static double getEntropyOfADataSet(JavaRDD<PojoRow> trainingData) {
		// entropy of whole data set
		// change from labeledPointJavaRDD to testdata
		JavaRDD<PojoRow> rddOfPositiveTweets = trainingData.filter(new Function<PojoRow, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					return true;
				}
				return false;
			}
		});
		long numOfNegativeTweets = trainingData.count() - rddOfPositiveTweets.count();
		Double possibilityOfPositive = Double.longBitsToDouble(rddOfPositiveTweets.count())
				/ Double.longBitsToDouble(trainingData.count());
		Double possibilityOfNegative = Double.longBitsToDouble(numOfNegativeTweets)
				/ Double.longBitsToDouble(trainingData.count());
		Double entropyOfDataset = -((possibilityOfPositive) * (Math.log(possibilityOfPositive) / Math.log(2))
				+ ((possibilityOfNegative) * (Math.log(possibilityOfNegative) / Math.log(2))));
		return entropyOfDataset;

	}

}
