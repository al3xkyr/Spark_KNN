package org.sparkexample.classifiers;

import java.io.Serializable;
import java.util.Vector;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.linalg.Vectors;
import org.sparkexample.pojo.PojoRow;

public class InitialParameters implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private  double[] possibilityOfXEq1givenCgood ;
	private  double[] possibilityOfXEq1givenCbad ;
	private  double[] possibilityOfXEq0givenCgood ;
	private  double[] possibilityOfXEq0givenCbad ;
	
	
	private double posCgood;
	private double posCbad;
	private double goodTweetNumber;
	private double badTweetNumber;
	public InitialParameters (JavaRDD<PojoRow> trainingData){
		constructPC(trainingData);
	}

	
	
	
	private void constructPC(JavaRDD<PojoRow> trainingData){
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

		this.goodTweetNumber = goodTweetMap.count();
		this.badTweetNumber = badTweetMap.count();
		// p(c=1) pithanotita twn kalwn tweets sto set
		long goodTweetMapNumber = goodTweetMap.count();
		long badTweetMapNumber = badTweetMap.count();
		final Double probabilityCequalsGoodtweet = goodTweetMap.count() / trainingDataCount;
		final Double probabilityCequalsBadTweet = 1 - probabilityCequalsGoodtweet;
		this.posCgood = probabilityCequalsGoodtweet;
		this.posCbad = probabilityCequalsBadTweet;

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
		
		this.possibilityOfXEq1givenCgood = addPosibilitiesBasedOnSumsTweets(pojoOfSumOfGoodTweets, goodTweetMapNumber, true).features.toArray();
		this.possibilityOfXEq0givenCgood = addPosibilitiesBasedOnSumsTweets(pojoOfSumOfGoodTweets, goodTweetMapNumber, false).features.toArray();
		this.possibilityOfXEq1givenCbad = addPosibilitiesBasedOnSumsTweets(pojoOfSumOfBadTweets, badTweetMapNumber, true).features.toArray();
		this.possibilityOfXEq0givenCbad =  addPosibilitiesBasedOnSumsTweets(pojoOfSumOfGoodTweets, badTweetMapNumber, false).features.toArray();
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




	public double[] getPossibilityOfXEq1givenCgood() {
		return possibilityOfXEq1givenCgood;
	}




	public void setPossibilityOfXEq1givenCgood(double[] possibilityOfXEq1givenCgood) {
		this.possibilityOfXEq1givenCgood = possibilityOfXEq1givenCgood;
	}




	public double[] getPossibilityOfXEq1givenCbad() {
		return possibilityOfXEq1givenCbad;
	}




	public void setPossibilityOfXEq1givenCbad(double[] possibilityOfXEq1givenCbad) {
		this.possibilityOfXEq1givenCbad = possibilityOfXEq1givenCbad;
	}




	public double[] getPossibilityOfXEq0givenCgood() {
		return possibilityOfXEq0givenCgood;
	}




	public void setPossibilityOfXEq0givenCgood(double[] possibilityOfXEq0givenCgood) {
		this.possibilityOfXEq0givenCgood = possibilityOfXEq0givenCgood;
	}




	public double[] getPossibilityOfXEq0givenCbad() {
		return possibilityOfXEq0givenCbad;
	}




	public void setPossibilityOfXEq0givenCbad(double[] possibilityOfXEq0givenCbad) {
		this.possibilityOfXEq0givenCbad = possibilityOfXEq0givenCbad;
	}




	public double getPosCgood() {
		return posCgood;
	}




	public void setPosCgood(double posCgood) {
		this.posCgood = posCgood;
	}




	public double getPosCbad() {
		return posCbad;
	}




	public void setPosCbad(double posCbad) {
		this.posCbad = posCbad;
	}




	public double getGoodTweetNumber() {
		return goodTweetNumber;
	}




	public void setGoodTweetNumber(double goodTweetNumber) {
		this.goodTweetNumber = goodTweetNumber;
	}




	public double getBadTweetNumber() {
		return badTweetNumber;
	}




	public void setBadTweetNumber(double badTweetNumber) {
		this.badTweetNumber = badTweetNumber;
	}
}
