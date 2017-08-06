package org.sparkexample.classifiers1;

import java.util.List;

import org.sparkexample.pojo1.PojoRow;

public class AmmendManager {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private double[] possibilityOfXEq1givenCgood;
	private double[] possibilityOfX흎1givenCbad;
	private double[] possibilityOfXEq0givenCgood;
	private double[] possibilityOfX흎0givenCbad;

	private double posCgood;
	private double posCbad;
	private double numberOfCgood;
	private double numberOfCbad;

	public AmmendManager(double[] possibilityOfXEq1givenCgood, double[] possibilityOfX흎1givenCbad,
			double[] possibilityOfXEq0givenCgood, double[] possibilityOfX흎0givenCbad, double posCgood, double posCbad,
			double numberOfCgood, double numberOfCbad) {
		this.possibilityOfXEq1givenCgood = possibilityOfXEq1givenCgood;
		this.possibilityOfXEq0givenCgood = possibilityOfXEq0givenCgood;
		this.possibilityOfX흎0givenCbad = possibilityOfX흎0givenCbad;
		this.possibilityOfX흎1givenCbad = possibilityOfX흎1givenCbad;
		this.posCbad = posCbad;
		this.posCgood = posCgood;
		this.numberOfCgood = numberOfCgood;
		this.numberOfCbad = numberOfCbad;
	}

	public void ammend(List<PojoRow> listOfAmmendingTweets, double countOfTrainingData) {
		double s = countOfTrainingData + listOfAmmendingTweets.size();
		for (PojoRow tweet : listOfAmmendingTweets) {
			if (tweet.label == 1.0) {
				this.numberOfCgood = this.numberOfCgood + 1;
				this.posCgood = ((s / (s + 1)) * this.posCgood) + (1 / (s + 1));
				this.posCbad = (s / (s + 1)) * this.posCbad;
				this.setPXc(tweet);
			} else {
				this.numberOfCbad = this.numberOfCbad + 1;
				this.posCgood = ((s / (s + 1)) * this.posCgood);
				this.posCbad = ((s / (s + 1)) * this.posCbad) + (1 / (s + 1));
			}
		}
	}

	public void setPXc(PojoRow tweet) {
		if (tweet.label == 1.0) {
			double m = 2 + this.numberOfCgood;
			double[] tweetFeatures = tweet.features.toArray();
			for (int i = 0; i < tweetFeatures.length; i++) {
				if (tweetFeatures[i] == 1) {
					// for good tweets with 1
					double tmp1 = this.possibilityOfXEq1givenCgood[i];
					double newTmp1 = ((m / (m + 1)) * tmp1) + (1 / (m + 1));
					this.possibilityOfXEq1givenCgood[i] = newTmp1;
					// for good tweets with 0
					double tmp0 = this.possibilityOfXEq0givenCgood[i];
					double newTmp0 = ((m / (m + 1)) * tmp0);
					this.possibilityOfXEq0givenCgood[i] = newTmp0;
				}
			}
		} else {
			double m = 2 + this.numberOfCbad;
			double[] tweetFeatures = tweet.features.toArray();
			for (int i = 0; i < tweetFeatures.length; i++) {
				if (tweetFeatures[i] == 1) {
					// for good tweets with 1
					double tmp1 = this.possibilityOfX흎1givenCbad[i];
					double newTmp1 = ((m / (m + 1)) * tmp1) + (1 / (m + 1));
					this.possibilityOfX흎1givenCbad[i] = newTmp1;
					// for good tweets with 0
					double tmp0 = this.possibilityOfX흎0givenCbad[i];
					double newTmp0 = ((m / (m + 1)) * tmp0);
					this.possibilityOfX흎0givenCbad[i] = newTmp0;
				}
			}
		}
	}

	public double[] getPossibilityOfXEq1givenCgood() {
		return possibilityOfXEq1givenCgood;
	}

	public double[] getPossibilityOfX흎1givenCbad() {
		return possibilityOfX흎1givenCbad;
	}

	public double[] getPossibilityOfXEq0givenCgood() {
		return possibilityOfXEq0givenCgood;
	}

	public double[] getPossibilityOfX흎0givenCbad() {
		return possibilityOfX흎0givenCbad;
	}

	public double getPosCgood() {
		return posCgood;
	}

	public double getPosCbad() {
		return posCbad;
	}
}
