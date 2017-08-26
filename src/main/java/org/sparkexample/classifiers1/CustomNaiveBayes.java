package org.sparkexample.classifiers1;

import java.util.ArrayList;
import java.util.List;

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
		JavaRDD<PojoRow>[] splits = data.getDatalabeledPoint().randomSplit(new double[] { 0.5, 0.1, 0.2, 0.2,});

		JavaRDD<PojoRow> trainingData = splits[0];
		JavaRDD<PojoRow> initTestData = splits[1]; 

		JavaRDD<PojoRow> ammendedDataForOptimalAccuracy = splits[2];
		JavaRDD<PojoRow> ammendedDataForKNNAccuracy = splits[3];
		


		
		 InitialParameters baseModel = new InitialParameters(trainingData);
		 KNNClassification knnClassifier = new KNNClassification(trainingData, sc1);
		 List<PojoRow> rightDataforAmmending = ammendedDataForOptimalAccuracy.collect();
		 AmmendManager ammendWithRightdata = new AmmendManager(baseModel.possibilityOfXEq1givenCgood
				, baseModel.possibilityOfX흎1givenCbad, 
				baseModel.possibilityOfXEq0givenCgood, 
				baseModel.possibilityOfX흎0givenCbad, 
				baseModel.posCgood, 
				baseModel.posCbad, baseModel.goodTweetNumber, baseModel.badTweetNumber,
				rightDataforAmmending, 
				trainingData.count());
		
		 List<PojoRow> knnDataForAmmending = knnClassifier.getPredictedRDD(ammendedDataForKNNAccuracy);
		 AmmendManager ammendWithKNNdata = new AmmendManager(baseModel.possibilityOfXEq1givenCgood
				, baseModel.possibilityOfX흎1givenCbad, 
				baseModel.possibilityOfXEq0givenCgood, 
				baseModel.possibilityOfX흎0givenCbad, 
				baseModel.posCgood, 
				baseModel.posCbad, baseModel.goodTweetNumber, baseModel.badTweetNumber,
				knnDataForAmmending, 
				trainingData.count());
		 
		 List<PojoRow> crossValidatedData = getCrossValidatedTweets(knnDataForAmmending, baseModel);
		 AmmendManager ammendWithCrossValidatedData = new AmmendManager(baseModel.possibilityOfXEq1givenCgood
				, baseModel.possibilityOfX흎1givenCbad, 
				baseModel.possibilityOfXEq0givenCgood, 
				baseModel.possibilityOfX흎0givenCbad, 
				baseModel.posCgood, 
				baseModel.posCbad, baseModel.goodTweetNumber, baseModel.badTweetNumber,
				crossValidatedData,
				trainingData.count());
		
		// --------------end of sums ---------------and probabilities
		// calculating accuracy of Naive old model in testDataForValidation
		List<PojoRow> initTestDataCollected = initTestData.collect();
		double accuracyOfInitModel = getAccuracyOnaSetWithInitalParameter(initTestDataCollected, baseModel);
		double acccuracyOfAmmendedGoodData = getAccuracyOnaSetWithAmmendedManager(initTestDataCollected, ammendWithRightdata);
		double accuracyOfAmmendedWithKNNPredictions = getAccuracyOnaSetWithAmmendedManager(initTestDataCollected, ammendWithKNNdata);
		double accuracyOfAmmendedWithCrossValidatedData = getAccuracyOnaSetWithAmmendedManager(initTestDataCollected, ammendWithCrossValidatedData);
		///==================================Ideal scenario of ammending good data======================================================
		
		
		/// ==================================================Scenario of KNN data =================================================
		
		System.out.println("The accuracy of Old naive model with dataForValidation is "
				+ accuracyOfInitModel);
		
		System.out.println("The optimal accuracy after the ammend of ammendedDataForOptimalAccuracy " + acccuracyOfAmmendedGoodData);
			
		
		System.out.println( "The accuracy after the ammend from KNN classifier " + accuracyOfAmmendedWithKNNPredictions);
		
		System.out.println("The accuracy after the data validated is " + accuracyOfAmmendedWithCrossValidatedData);

		}
	
	public static double getAccuracyOnaSetWithInitalParameter(List<PojoRow> list, InitialParameters baseModel){
		double accuracy = 0; 
		double countOfCorrect = 0;
		for ( PojoRow p : list){
			double toDouble = p.label; 
			double prediction = NafiBayesModel.classify(
					baseModel.possibilityOfXEq1givenCgood, baseModel.possibilityOfXEq0givenCgood,
					baseModel.possibilityOfX흎1givenCbad, baseModel.possibilityOfX흎0givenCbad,
					baseModel.posCgood,
					baseModel.posCbad, p.features.toArray()); 
			if ( prediction == toDouble){
				countOfCorrect ++;
			}
		}
		accuracy = countOfCorrect/(double)list.size();
		
		return accuracy;
	}
	public static double getAccuracyOnaSetWithAmmendedManager(List<PojoRow> list, AmmendManager ammendedClassifier){
		double accuracy = 0; 
		double countOfCorrect = 0;
		
		for ( PojoRow p : list){
			double toDouble = p.label;
			double prediction = NafiBayesModel.classify(
					ammendedClassifier.possibilityOfXEq1givenCgood, ammendedClassifier.possibilityOfXEq0givenCgood,
					ammendedClassifier.possibilityOfX흎1givenCbad, ammendedClassifier.possibilityOfX흎0givenCbad,
					ammendedClassifier.posCgood,
					ammendedClassifier.posCbad, p.features.toArray());
			if (  prediction == toDouble){
				countOfCorrect ++;
			}
		}
		accuracy = countOfCorrect/(double)list.size();
		return accuracy;

	}
	
 public static List<PojoRow> getCrossValidatedTweets(List<PojoRow> list, InitialParameters baseModel){
	 List<PojoRow> validatedList = new ArrayList<PojoRow>();
	 for (PojoRow pojo : list){
		 
		if( NafiBayesModel.classify(
				 baseModel.possibilityOfXEq1givenCgood, baseModel.possibilityOfXEq0givenCgood,
				 baseModel.possibilityOfX흎1givenCbad, baseModel.possibilityOfX흎0givenCbad,
				 baseModel.posCgood,
				 baseModel.posCbad, pojo.features.toArray())== pojo.label){
		validatedList.add(pojo);	
		}
		
	 }
	 return validatedList;
	 
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
