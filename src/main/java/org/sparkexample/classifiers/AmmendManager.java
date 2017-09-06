package org.sparkexample.classifiers;

import java.io.Serializable;
import java.util.List;

import org.sparkexample.pojo.PojoRow;

public class AmmendManager {
	private double[] possibilityOfXEq1givenCgood;
	private double[] possibilityOfXEq1givenCbad;
	private double[] possibilityOfXEq0givenCgood;
	private double[] possibilityOfXEq0givenCbad;

	private double posCgood;
	private double posCbad;
	private double numberOfCgood;
	private double numberOfCbad;

	public AmmendManager(double[] possibilityOfXEq1givenCgood, double[] possibilityOfXEq1givenCbad,
			double[] possibilityOfXEq0givenCgood, double[] possibilityOfXEq0givenCbad, double posCgood, double posCbad,
			double numberOfCgood, double numberOfCbad,List<PojoRow> dataforAmmending, long trainingDatacount) {
		this.possibilityOfXEq1givenCgood = possibilityOfXEq1givenCgood;
		this.possibilityOfXEq0givenCgood = possibilityOfXEq0givenCgood;
		this.possibilityOfXEq0givenCbad = possibilityOfXEq0givenCbad;
		this.possibilityOfXEq1givenCbad = possibilityOfXEq1givenCbad;
		this.posCbad = posCbad;
		this.posCgood = posCgood;
		this.numberOfCgood = numberOfCgood;
		this.numberOfCbad = numberOfCbad;
		ammend(dataforAmmending, trainingDatacount);
	}

	private void ammend(List<PojoRow> listOfAmmendingTweets, long countOfTrainingData) {
		Long long1 = new Long(countOfTrainingData);
		double countOfTrainingDataDouble = long1.doubleValue();
		double s = countOfTrainingDataDouble + listOfAmmendingTweets.size();
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

	private void setPXc(PojoRow tweet) {
		if (tweet.label == 1.0) {
			double m = 2 + this.numberOfCgood;
			double[] tweetFeatures = tweet.features.toArray();
			for (int i = 0; i < tweetFeatures.length; i++) {
				if (tweetFeatures[i] == 1) {
					// for good tweets with 1
					double tmp1 = this.possibilityOfXEq1givenCgood[i];
					double newTmp1 = ((m / (double)(m + 1)) * tmp1) + (1 / (double)(m + 1));
					this.possibilityOfXEq1givenCgood[i] = newTmp1;
					// for good tweets with 0
					double tmp0 = this.possibilityOfXEq0givenCgood[i];
					double newTmp0 = ((m / (double) (m + 1)) * tmp0);
					this.possibilityOfXEq0givenCgood[i] = newTmp0;
				}
			}
		} else {
			double m = 2 + this.numberOfCbad;
			double[] tweetFeatures = tweet.features.toArray();
			for (int i = 0; i < tweetFeatures.length; i++) {
				if (tweetFeatures[i] == 1) {
					// for good tweets with 1
					double tmp1 = this.possibilityOfXEq1givenCbad[i];
					double newTmp1 = ((m / (double)(m + 1)) * tmp1) + (1 / (double)(m + 1));
					this.possibilityOfXEq1givenCbad[i] = newTmp1;
					// for good tweets with 0
					double tmp0 = this.possibilityOfXEq0givenCbad[i]; 
					double newTmp0 = ((m / (double)(m + 1)) * tmp0);
					this.possibilityOfXEq0givenCbad[i] = newTmp0;
				}
			}
		}
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

	public double getNumberOfCgood() {
		return numberOfCgood;
	}

	public void setNumberOfCgood(double numberOfCgood) {
		this.numberOfCgood = numberOfCgood;
	}

	public double getNumberOfCbad() {
		return numberOfCbad;
	}

	public void setNumberOfCbad(double numberOfCbad) {
		this.numberOfCbad = numberOfCbad;
	}

	
}
