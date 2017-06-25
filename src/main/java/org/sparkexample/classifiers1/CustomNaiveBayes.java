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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.sparkexample.pojo1.DataFeature;
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
		String datapath = "data/60000tweets.csv";

		JavaRDD<String> dataAsStrings = sc1.textFile(datapath, 5).toJavaRDD();
		// transformation on dataset to be cleaned and create the datafeature
		// which has swn score and the attributes that i want to store
		// pernei to rdd apo panw kai to kanei apo RDD string se RDD apo
		// dataFeature

		JavaRDD<DataFeature> cleaned = dataAsStrings.map(new Function<String, DataFeature>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public DataFeature call(String s) throws Exception {
				DataFeature dataFeature = new DataFeature();

				String json2 = s.substring(1, s.lastIndexOf("}") + 1);

				String result = s.substring(s.length() - 3, s.length()).replace(",", "").replace("\"", "");
				int lastrow = Integer.valueOf(result);
				double lable;
				if (lastrow == -1) {
					lable = 0;
				} else {
					lable = 1;
				}
				JSONObject json1 = null;
				Double swn = null;
				try {
					json1 = new JSONObject(json2);
					swn = json1.getDouble("__swn_score__");
					dataFeature.setData(json1);
					dataFeature.setLable(lable);
				} catch (JSONException e) {
					e.printStackTrace();
					return null;

				}

				return dataFeature;
			}
		}).cache(); // edw ta krataei stin mnimi

		JavaRDD<PojoRow> labeledPointJavaRDD = cleaned.map(new Function<DataFeature, PojoRow>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public PojoRow call(DataFeature dataFeature) throws Exception {

				double neg;
				boolean negation = dataFeature.getData().getBoolean("__NEGATION__");
				if (negation) {
					neg = 1;
				} else {
					neg = 0;
				}
				double negSm;
				boolean negSmile = dataFeature.getData().getBoolean("__NEG_SMILEY__");
				if (negSmile) {
					negSm = 1;
				} else {
					negSm = 0;
				}

				double posSm;
				boolean pos = dataFeature.getData().getBoolean("__POS_SMILEY__");
				if (pos) {
					posSm = 1;

				} else {
					posSm = 0;
				}
				double ohso;
				boolean ohsoBool = dataFeature.getData().getBoolean("__OH_SO__");
				if (ohsoBool) {
					ohso = 1;

				} else {
					ohso = 0;
				}
				double capital;
				boolean capitalBool = dataFeature.getData().getBoolean("__CAPITAL__");
				if (capitalBool) {
					capital = 1;

				} else {
					capital = 0;
				}
				double isMetaphor;
				boolean isMetaphorBool = dataFeature.getData().getBoolean("__is_metaphor__");
				if (isMetaphorBool) {
					isMetaphor = 1;
				} else {
					isMetaphor = 0;
				}
				// ,neg,ohso,capital
				double love;
				boolean loveBool = dataFeature.getData().getBoolean("__LOVE__");
				if (loveBool) {
					love = 1;
				} else {
					love = 0;
				}
				// breaking the double value for total swn score based on the
				// observations about the polarity of the tweet by means
				double swn_positive = 0;
				double swn_negative = 0;
				double swn_somewhatPositive = 0;
				double swn_neutural = 0;
				double swn_somewhatNegative = 0;
				double swnScore = dataFeature.getData().getDouble("__swn_score__");
				if (swnScore > 1.2) {
					swn_positive = 1;
				} else if (swnScore <= 1.2 && swnScore > 0.95) {
					swn_somewhatPositive = 1;
				} else if (swnScore <= 0.95 && swnScore > 0.5) {
					swn_neutural = 1;
				} else if (swnScore <= 0.5 && swnScore > 0.2) {
					swn_somewhatNegative = 1;
				} else if (swnScore < 0.2) {
					swn_negative = 1;
				}
				// epilegontai features pou den einai eutheos analoga me ton
				// prosdiorismo tis klasis tou tweet
				// diladi ta positive smileys kai ta negatives einai amesa
				// sindedemena me tin akrivia tou classifier
				double[] arrayofVector = { swn_positive, swn_negative, swn_somewhatPositive, swn_neutural,
						swn_somewhatNegative, neg, ohso, capital, isMetaphor, love };
				Vector dv = Vectors.dense(arrayofVector);
				PojoRow rowPojoforProcess = new PojoRow(dataFeature.getLable(), dv);
				return rowPojoforProcess;
			}
		});

		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<PojoRow>[] splits = labeledPointJavaRDD.randomSplit(new double[] { 0.7, 0.1, 0.2 });

		JavaRDD<PojoRow> trainingData = splits[0];
		JavaRDD<PojoRow> testData = splits[1]; // arxiki morfi , meta allaxtikan
												// apo ton knn me
												// traindataCollectedwithLabelFromKNNFromTestData
												// san lista

		JavaRDD<PojoRow> testDataForValidation = splits[2];

		// have to put in type of < Integer , PojoRow > where
		// Integer is the nth element of feature array Pojorow contains the
		// array

		// In order to calculate model of naive bayes
		// we want to calculate
		// P(C|X) = P(X|C)*P(C) / P(X) for each new tweet for each class ( good
		// , bad )
		// On given tweet that has the biggest posterior probability on a class
		// assigned there
		// Naive model

		double trainingDataCount = trainingData.count();
		JavaRDD<PojoRow> goodTweetMap = trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					return true;
				}
				return false;
			}
		}).cache();
		JavaRDD<PojoRow> badTweetMap = trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					return true;
				}
				return false;
			}
		}).cache();

		// p(c=1) pithanotita twn kalwn tweets sto set
		long goodTweetMapNumber = goodTweetMap.count();
		long badTweetMapNumber = badTweetMap.count();
		final Double probabilityCequalsGoodtweet = goodTweetMap.count() / trainingDataCount;
		final Double probabilityCequalsBadTweet = 1 - probabilityCequalsGoodtweet;

		// In order to decouple algorithm from the specific features
		// i have to hold it on an array of
		PojoRow pojoOfSumOfGoodTweets = goodTweetMap.reduce(new Function2<PojoRow, PojoRow, PojoRow>() {

			public PojoRow call(PojoRow v1, PojoRow v2) throws Exception {
				// TODO Auto-generated method stub
				double[] v1Array = v1.features.toArray();
				double[] v2Array = v2.features.toArray();
				double[] vSumArray = new double[v1.features.size()];
				for (int i = 0; i < v1Array.length; i++) {
					vSumArray[i] = v1Array[i] + v2Array[i];
				}
				return new PojoRow(v1.label, Vectors.dense(vSumArray));
			}
		});
		PojoRow pojoOfSumOfBadTweets = badTweetMap.reduce(new Function2<PojoRow, PojoRow, PojoRow>() {

			public PojoRow call(PojoRow v1, PojoRow v2) throws Exception {
				// TODO Auto-generated method stub
				double[] v1Array = v1.features.toArray();
				double[] v2Array = v2.features.toArray();
				double[] vSumArray = new double[v1.features.size()];
				for (int i = 0; i < v1Array.length; i++) {
					vSumArray[i] = v1Array[i] + v2Array[i];
				}
				return new PojoRow(v1.label, Vectors.dense(vSumArray));
			}
		});
		PojoRow pojoOfProbabilitiesOfGoodTweets = addPosibilitiesBasedOnSumsforGoodTweets(pojoOfSumOfGoodTweets,
				goodTweetMapNumber);
		PojoRow pojoOfProbabilitiesOfBadTweets = addPosibilitiesBasedOnSumsforBadTweets(pojoOfSumOfBadTweets,
				badTweetMapNumber);

		// --------------end of sums ---------------and probabilities
		// calculating accuracy of Naive old model in testDataForValidation
		JavaPairRDD<Double, Double> predictionAndLabelForTestDataForValidationwithOldModel = testData
				.mapToPair(new PairFunction<PojoRow, Double, Double>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Double> call(PojoRow p) {
						return new Tuple2<Double, Double>(NafiBayesModel.classify(p.getfeatures().toArray(),
								probabilityCequalsGoodtweet, probabilityCequalsBadTweet), p.getLabel());
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

		JavaRDD<PojoRow> testDataForValidationWithOldModelPredicted = testData
				.map(new Function<PojoRow, PojoRow>() {

					public PojoRow call(PojoRow v1) throws Exception {
						// TODO Auto-generated method stub
						return new PojoRow(NafiBayesModel.classify(v1.getfeatures().toArray(),
								probabilityCequalsGoodtweet, probabilityCequalsBadTweet), v1.features);
					}
				});
		if (doesModelNeedRetrain(trainingData, testDataForValidationWithOldModelPredicted)) {

			// ===============================================================================================================================================================
			Dataset<Row> labeledPointDataset = sqlSpark.createDataFrame(trainingData, PojoRow.class);
			// Knn classification
			KNNClassifier val = new KNNClassifier().setTopTreeSize(2).setK(5);
			val.train(labeledPointDataset);

			KNNClassificationModel model = val.fit(labeledPointDataset);
			// predict on label point for Dataset
			// val.transform(labeledPointDataset, model.topTree(),
			// model.subTrees());
			// Creation of Dataset of testData for KNN
			Dataset<Row> labeledPointDatasetforTest = sqlSpark.createDataFrame(testDataForValidation, PojoRow.class);

			Dataset<Row> sss = model.transform(labeledPointDatasetforTest);
			sss.show();
			Dataset<Row> j32 = sss.select("features", "label", "prediction");
			j32.show();
			JavaPairRDD<Double, Double> ewo = j32.select("prediction", "label").toJavaRDD()
					.mapToPair(new PairFunction<Row, Double, Double>() {

						public Tuple2<Double, Double> call(Row t) throws Exception {
							// TODO Auto-generated method stub
							return new Tuple2<Double, Double>(t.getDouble(0), t.getDouble(1));
						}
					});
			double accuracyOfKNNOntestDataForValidation = ewo.filter(new Function<Tuple2<Double, Double>, Boolean>() {

				public Boolean call(Tuple2<Double, Double> v1) throws Exception {
					// TODO Auto-generated method stub
					if (v1._1.doubleValue() == v1._2.doubleValue()) {
						return true;
					}
					return false;
				}
			}).count() / (double) (testDataForValidation.count());
			System.out.println("the accuaracy of knn is " + accuracyOfKNNOntestDataForValidation);

			// creating new rdd with label the prediction on test data
			JavaRDD<PojoRow> trainDataForNaiveWithPredictionsOfKNN = j32.select("prediction", "features").toJavaRDD()
					.map(new Function<Row, PojoRow>() {

						public PojoRow call(Row t) throws Exception {
							// TODO Auto-generated method stub
							return new PojoRow((Double) t.get(0), (Vector) t.get(1));
						}
					});
			List<PojoRow> traindataCollectedwithLabelFromKNNFromTestData = trainDataForNaiveWithPredictionsOfKNN
					.collect();
			// List<PojoRow> traindataCollectedwithLabelFromKNNFromTestData =
			// testData.collect();
			// end of knn

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
					NafiBayesModel.possibilityOfXgivenCgood.clear();

					probabilityCequalsGoodtweetAmmendedbytestDatafromKNN = (((double) newCountOfTrainingData
							/ ((double) newCountOfTrainingData + 1))
							* probabilityCequalsGoodtweetAmmendedbytestDatafromKNN)
							+ (double) (1 / (newCountOfTrainingData + 1));

					probabilityCequalsBadtweetAmmendedbytestDatafromKNN = (double) ((double) newCountOfTrainingData
							/ ((double) newCountOfTrainingData + 1))
							* (double) probabilityCequalsBadtweetAmmendedbytestDatafromKNN;
					for (int i = 0; i < pj.features.toArray().length; i++) {
						if (pj.features.toArray()[i] == 1.00) {
							pojoOfProbabilitiesOfGoodTweets.features
									.toArray()[i] = (double) pojoOfProbabilitiesOfGoodTweets.features.toArray()[i]
											* (m / (m + 1)) + (1 / (1 + m));
							NafiBayesModel.possibilityOfXgivenCgood
									.add((double) pojoOfProbabilitiesOfGoodTweets.features.toArray()[i]);
						} else {
							pojoOfProbabilitiesOfGoodTweets.features
									.toArray()[i] = (double) pojoOfProbabilitiesOfGoodTweets.features.toArray()[i]
											* (m / (m + 1));
							NafiBayesModel.possibilityOfXgivenCgood
									.add((double) pojoOfProbabilitiesOfGoodTweets.features.toArray()[i]);
						}
					}

				} else {
					NafiBayesModel.possibilityOfXgivenCbad.clear();

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
							NafiBayesModel.possibilityOfXgivenCbad
									.add((double) pojoOfProbabilitiesOfBadTweets.features.toArray()[i]);
						} else {
							pojoOfProbabilitiesOfBadTweets.features
									.toArray()[i] = (double) pojoOfProbabilitiesOfBadTweets.features.toArray()[i]
											* (m2 / (m2 + 1));
							NafiBayesModel.possibilityOfXgivenCbad
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

			System.out.println("The accuracy of Old naive model with dataForValidation is "
					+ acccuracyOnTestDataWithOldModel);
			System.out.println("The accuracy of new naive model with dataForValidation is "
					+ accuracyonTestDataForValidationwithNewModel);

			// end of ammend
		}
	}

	/**
	 * Add to a PojoRow the posibilities of each feature / count of features
	 * that have bad label add to NafiBayesModel those features
	 * 
	 * @param pojoOfSumOfBadTweets
	 * @param badTweetMapNumber
	 * @return
	 */

	private static PojoRow addPosibilitiesBasedOnSumsforBadTweets(PojoRow pojoOfSumOfBadTweets,
			long badTweetMapNumber) {
		// TODO Auto-generated method stub
		double[] arrayOfFeatures = pojoOfSumOfBadTweets.features.toArray();
		double[] arrayOfProbabilitites = new double[arrayOfFeatures.length];
		NafiBayesModel.possibilityOfXgivenCbad.clear();
		for (int i = 0; i < arrayOfFeatures.length; i++) {
			arrayOfProbabilitites[i] = arrayOfFeatures[i] / badTweetMapNumber;
			NafiBayesModel.possibilityOfXgivenCbad.add(arrayOfFeatures[i] / badTweetMapNumber);
		}
		return new PojoRow(pojoOfSumOfBadTweets.label, Vectors.dense(arrayOfProbabilitites));
	}

	/**
	 * Add to a PojoRow the posibilities of each feature / count of features
	 * that have good label add to NafiBayesModel those features
	 * 
	 * @param pojoOfSumOfGoodTweets
	 * @param goodTweetMapNumber
	 * @return
	 */
	private static PojoRow addPosibilitiesBasedOnSumsforGoodTweets(PojoRow pojoOfSumOfGoodTweets,
			long goodTweetMapNumber) {
		// TODO Auto-generated method stub
		double[] arrayOfFeatures = pojoOfSumOfGoodTweets.features.toArray();
		double[] arrayOfProbabilitites = new double[arrayOfFeatures.length];
		NafiBayesModel.possibilityOfXgivenCgood.clear();
		for (int i = 0; i < arrayOfFeatures.length; i++) {
			arrayOfProbabilitites[i] = arrayOfFeatures[i] / goodTweetMapNumber;
			NafiBayesModel.possibilityOfXgivenCgood.add(arrayOfFeatures[i] / goodTweetMapNumber);
		}

		return new PojoRow(pojoOfSumOfGoodTweets.label, Vectors.dense(arrayOfProbabilitites));
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
