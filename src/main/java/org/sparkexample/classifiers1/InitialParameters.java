package org.sparkexample.classifiers1;

import java.io.Serializable;
import java.util.Vector;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.linalg.Vectors;
import org.sparkexample.pojo1.PojoRow;

public class InitialParameters implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public  double[] possibilityOfXEq1givenCgood ;
	public  double[] possibilityOfX흎1givenCbad ;
	public  double[] possibilityOfXEq0givenCgood ;
	public  double[] possibilityOfX흎0givenCbad ;
	
	
	public double posCgood;
	public double posCbad;
	public InitialParameters (JavaRDD<PojoRow> trainingData){
		constructPC(trainingData);
	}

	
	
	
	public void constructPC(JavaRDD<PojoRow> trainingData){
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
		posCgood = probabilityCequalsGoodtweet;
		posCbad = probabilityCequalsBadTweet;

		// It counts the values of 1 on P(x|c) in order to P(A=1|c=positive) 
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
		// Assign it to 
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
		
		possibilityOfXEq1givenCgood = addPosibilitiesBasedOnSumsTweets(pojoOfSumOfGoodTweets, goodTweetMapNumber, true).features.toArray();
		possibilityOfXEq0givenCgood = addPosibilitiesBasedOnSumsTweets(pojoOfSumOfGoodTweets, goodTweetMapNumber, false).features.toArray();
		possibilityOfX흎1givenCbad = addPosibilitiesBasedOnSumsTweets(pojoOfSumOfBadTweets, badTweetMapNumber, true).features.toArray();
		possibilityOfX흎0givenCbad =  addPosibilitiesBasedOnSumsTweets(pojoOfSumOfGoodTweets, badTweetMapNumber, false).features.toArray();
	}
	/**
	 * This method exist to calclulate the initial P(X|C) 
	 * @param pojoOfSumOfGoodTweets
	 * @param goodTweetMapNumber
	 * @param forOne
	 * @return
	 */
	private static PojoRow addPosibilitiesBasedOnSumsTweets(PojoRow pojoOfSumOfGoodTweets,
			long goodTweetMapNumber , boolean forOne) {
		// TODO Auto-generated method stub
		double[] arrayOfFeatures = pojoOfSumOfGoodTweets.features.toArray();
		double[] arrayOfProbabilitites = new double[arrayOfFeatures.length];
		if (forOne){
		for (int i = 0; i < arrayOfFeatures.length; i++) {
			arrayOfProbabilitites[i] = arrayOfFeatures[i] / goodTweetMapNumber;
		}}
		else {
			for (int i = 0; i < arrayOfFeatures.length; i++) {
				arrayOfProbabilitites[i] = 1- (arrayOfFeatures[i] / goodTweetMapNumber);
			}
		}
		
		return new PojoRow(pojoOfSumOfGoodTweets.label, Vectors.dense(arrayOfProbabilitites));
	}
}
