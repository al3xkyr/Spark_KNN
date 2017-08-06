package org.sparkexample.classifiers1;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
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
		JavaRDD<PojoRow>[] splits = data.getDatalabeledPoint().randomSplit(new double[] { 0.4, 0.1, 0.3, 0.1, 0.1 });

		JavaRDD<PojoRow> trainingData = splits[0];
		JavaRDD<PojoRow> initTestData = splits[1]; // arxiki morfi , meta allaxtikan
												// apo ton knn me
												// traindataCollectedwithLabelFromKNNFromTestData
												// san lista

		JavaRDD<PojoRow> ammendedDataForOptimalAccuracy = splits[2];
		JavaRDD<PojoRow> ammendedDataForKNNAccuracy = splits[3];
		


		
		final InitialParameters baseModel = new InitialParameters(trainingData);
		final AmmendManager ammendWithRightdata = new AmmendManager(baseModel.possibilityOfXEq1givenCgood
				, baseModel.possibilityOfX�q1givenCbad, 
				baseModel.possibilityOfXEq0givenCgood, 
				baseModel.possibilityOfX�q0givenCbad, 
				baseModel.posCgood, 
				baseModel.posCbad, baseModel.goodTweetNumber, baseModel.badTweetNumber,
				ammendedDataForOptimalAccuracy.collect(), 
				trainingData.count());
		
		
		
		// --------------end of sums ---------------and probabilities
		// calculating accuracy of Naive old model in testDataForValidation
		JavaPairRDD<Double, Double> predictionAndLabelForTestDataForValidationwithOldModel = initTestData
				.mapToPair(new PairFunction<PojoRow, Double, Double>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Double> call(PojoRow p) {
						return new Tuple2<Double, Double>(NafiBayesModel.classify(
								baseModel.possibilityOfXEq1givenCgood, baseModel.possibilityOfXEq0givenCgood,
								baseModel.possibilityOfX�q1givenCbad, baseModel.possibilityOfX�q0givenCbad,
								baseModel.posCgood,
								baseModel.posCbad, p.features.toArray()),p.label);
					}
				});
		double acccuracyOnTestDataWithOldModel = predictionAndLabelForTestDataForValidationwithOldModel
				.filter(new Function<Tuple2<Double, Double>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<Double, Double> pl) {
						return pl._1().equals(pl._2());
					}
				}).count() / (double) initTestData.count();
		
		///========================================================================================
		
		JavaPairRDD<Double, Double> mapAfterAmmendWithGoodData = initTestData
				.mapToPair(new PairFunction<PojoRow, Double, Double>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Double> call(PojoRow p) {
						return new Tuple2<Double, Double>(NafiBayesModel.classify(
								ammendWithRightdata.possibilityOfXEq1givenCgood, ammendWithRightdata.possibilityOfXEq0givenCgood,
								ammendWithRightdata.possibilityOfX�q1givenCbad, ammendWithRightdata.possibilityOfX�q0givenCbad,
								ammendWithRightdata.posCgood,
								ammendWithRightdata.posCbad, p.features.toArray()),p.label);
					}
				});
		double acccuracyOnTestDataWithAmmendedWithGoodData = mapAfterAmmendWithGoodData
				.filter(new Function<Tuple2<Double, Double>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<Double, Double> pl) {
						return pl._1().equals(pl._2());
					}
				}).count() / (double) initTestData.count();
		
		
		
		
		System.out.println("The accuracy of Old naive model with dataForValidation is "
				+ acccuracyOnTestDataWithOldModel);
		
		System.out.println("The optimal accuracy after the ammend of ammendedDataForOptimalAccuracy " + acccuracyOnTestDataWithAmmendedWithGoodData);
			

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
