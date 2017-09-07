package org.sparkexample.classifiers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.sparkexample.pojo.ExtendedPojoRow;
import org.sparkexample.pojo.PojoRow;

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
		// Training data 40k tweets
		String trainingDatapath = "data/60000tweetsTrain.csv";
		DataExtraction dataTrain = new DataExtraction(sc1, trainingDatapath);
		// Test and ammending data 20K tweets
		String testDatapath = "data/60000tweetsTest.csv";
		DataExtraction dataTest = new DataExtraction(sc1, testDatapath);

		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<PojoRow>[] splitsTrain = dataTrain.getDatalabeledPoint().randomSplit(new double[] { 0.9, 0.1});
		JavaRDD<PojoRow> trainingData = splitsTrain[0];
		JavaRDD<PojoRow>[] splits = dataTest.getDatalabeledPoint().randomSplit(new double[] { 0.5,  0.5});
		JavaRDD<PojoRow> initTestData = splits[0]; 

		JavaRDD<PojoRow> ammendedDataForKNNAccuracy = splits[1];
		

		// getting the test data on driver
		List<PojoRow> initTestDataCollected = initTestData.collect();
		// creating the initial model
		 final InitialParameters baseModel = new InitialParameters(trainingData);
		 System.out.println(baseModel.toString());
		 
		 // training the knn classifier
		 KNNClassification knnClassifier = new KNNClassification(trainingData, sc1);
		 
		 List<ExtendedPojoRow> knnDataForAmmending = knnClassifier.getPredictedRDD(ammendedDataForKNNAccuracy);
		 System.out.println("dummmy");
		 
		 // this  method returns the labels that had been predicted correctly
		 List<ExtendedPojoRow> crossValidatedData = getCrossValidatedTweets(knnDataForAmmending, baseModel);
		 double[] possibilitiesX1C1 = baseModel.getPossibilityOfXEq1givenCgood();
		 double[] possibilitiesX1C0 = baseModel.getPossibilityOfXEq1givenCbad();
		 double[] possibilitiesX0C1 = baseModel.getPossibilityOfXEq0givenCgood();
		 double[] possibilitiesX0C0 = baseModel.getPossibilityOfXEq0givenCbad();
		 double posGood = baseModel.getPosCgood();
		 double posBad = baseModel.getPosCbad();
		 double goodNumber = baseModel.getGoodTweetNumber();
		 double badNumber = baseModel.getGoodTweetNumber();
		 
		 // i take a List pojo row with correct label from the cross validated twets 
		 List<PojoRow> correctPojos = new ArrayList<PojoRow>();
		 for (ExtendedPojoRow extendPojo : crossValidatedData){
			 correctPojos.add(extendPojo.getPojo());
		 }
		 
		 // i take a List pojo row with label the prediction of KNN 
		 List<PojoRow> predictedPojos = new ArrayList<PojoRow>();
		 for (ExtendedPojoRow extendPojo : crossValidatedData){
			 predictedPojos.add(new PojoRow(extendPojo.getPrediction().doubleValue(), extendPojo.getPojo().getfeatures()));
		 }
		 
		 AmmendManager ammendWithCrossValidatedRightData = new AmmendManager(possibilitiesX1C1
					, possibilitiesX1C0, 
					possibilitiesX0C1, 
					possibilitiesX0C0, 
					posGood, 
					posBad, goodNumber, badNumber,
					correctPojos,
				trainingData.count());
		 
		 AmmendManager ammendWithCrossValidatedPredictionData = new AmmendManager(possibilitiesX1C1
					, possibilitiesX1C0, 
					possibilitiesX0C1, 
					possibilitiesX0C0, 
					posGood, 
					posBad, goodNumber, badNumber,
					predictedPojos,
				trainingData.count());
		 double accuracyOfInitModel = getAccuracyOnaSetWithInitalParameter(initTestDataCollected, baseModel);
		 double accuracyOfAmmendedWithRightCrossValidatedData = getAccuracyOnaSetWithAmmendedManager(initTestDataCollected, ammendWithCrossValidatedRightData);
		double accuracyOfAmmendedWithPredictedCrossvalidatedData = getAccuracyOnaSetWithAmmendedManager(initTestDataCollected, ammendWithCrossValidatedPredictionData);
		
		System.out.println("The inital accuracy on Testdata which has size :"+ initTestDataCollected.size() + "with size of training data "+ trainingData.count() + "is "+ accuracyOfInitModel);
		System.out.println("The ammended with Cross validated right data  accuracy on Testdata which has size :"+ initTestDataCollected.size() + "with size of ammended data "+ correctPojos.size() + "is "+ accuracyOfAmmendedWithRightCrossValidatedData);
		System.out.println("The ammended with Cross validated prediction data  accuracy on Testdata which has size :"+ initTestDataCollected.size() + "with size of ammended data "+ predictedPojos.size() + "is "+ accuracyOfAmmendedWithPredictedCrossvalidatedData);
		 
		 // geting corrected data for ammending
		 

		}
	
	public static double getAccuracyOnaSetWithInitalParameter(List<PojoRow> list, InitialParameters baseModel){
		double accuracy = 0; 
		double countOfCorrect = 0;
		for ( PojoRow p : list){
			double toDouble = p.label; 
			double prediction = NaiveBayesModel.classify(
					baseModel.getPossibilityOfXEq1givenCgood(), baseModel.getPossibilityOfXEq0givenCgood(),
					baseModel.getPossibilityOfXEq1givenCbad(), baseModel.getPossibilityOfXEq0givenCbad(),
					baseModel.getPosCgood(),
					baseModel.getPosCbad(), p.features.toArray()); 
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
			double prediction = NaiveBayesModel.classify(
					ammendedClassifier.getPossibilityOfXEq1givenCgood(), ammendedClassifier.getPossibilityOfXEq0givenCgood(),
					ammendedClassifier.getPossibilityOfXEq1givenCbad(), ammendedClassifier.getPossibilityOfXEq0givenCbad(),
					ammendedClassifier.getPosCgood(),
					ammendedClassifier.getPosCbad(), p.features.toArray());
			if (  prediction == toDouble){
				countOfCorrect ++;
			}
		}
		accuracy = countOfCorrect/(double)list.size();
		return accuracy;

	}
	
 public static List<ExtendedPojoRow> getCrossValidatedTweets(List<ExtendedPojoRow> list, InitialParameters baseModel){
	 List<ExtendedPojoRow> validatedList = new ArrayList<ExtendedPojoRow>();
	 for (ExtendedPojoRow pojo : list){
		 PojoRow pojorow = pojo.getPojo();
		if( NaiveBayesModel.classify(
				 baseModel.getPossibilityOfXEq1givenCgood(), baseModel.getPossibilityOfXEq0givenCgood(),
				 baseModel.getPossibilityOfXEq1givenCbad(), baseModel.getPossibilityOfXEq0givenCbad(),
				 baseModel.getPosCgood(),
				 baseModel.getPosCbad(), pojorow.features.toArray())== pojo.getPrediction()){
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
