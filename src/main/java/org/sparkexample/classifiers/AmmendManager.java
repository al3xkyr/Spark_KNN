package org.sparkexample.classifiers;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.sparkexample.pojo.PojoRow;

public class AmmendManager {
	private double[] possibilityOfXEq1givenCgood;
	private double[] possibilityOfX흎1givenCbad;
	private double[] possibilityOfXEq0givenCgood;
	private double[] possibilityOfX흎0givenCbad;

	private double posCgood;
	private double posCbad;
	private double numberOfCgood;
	private double numberOfCbad;

	private List<PojoRow> dataforAmmending;
	private long trainingDatacount;

	public AmmendManager(double[] possibilityOfXEq1givenCgood, double[] possibilityOfX흎1givenCbad,
			double[] possibilityOfXEq0givenCgood, double[] possibilityOfX흎0givenCbad, double posCgood, double posCbad,
			double numberOfCgood, double numberOfCbad, List<PojoRow> dataforAmmending, long trainingDatacount) {
		this.possibilityOfXEq1givenCgood = possibilityOfXEq1givenCgood;
		this.possibilityOfXEq0givenCgood = possibilityOfXEq0givenCgood;
		this.possibilityOfX흎0givenCbad = possibilityOfX흎0givenCbad;
		this.possibilityOfX흎1givenCbad = possibilityOfX흎1givenCbad;
		this.posCbad = posCbad;
		this.posCgood = posCgood;
		this.numberOfCgood = numberOfCgood;
		this.numberOfCbad = numberOfCbad;
		this.dataforAmmending = dataforAmmending;
		this.trainingDatacount = trainingDatacount;
		System.out.println("possibilityOfXEq1givenCgood1= "+Arrays.toString(this.possibilityOfXEq1givenCgood));
	}

	public void ammend() {
		Long long1 = new Long(trainingDatacount);
		double countOfTrainingDataDouble = long1.doubleValue();
		double s = countOfTrainingDataDouble + dataforAmmending.size();
		for (PojoRow tweet : dataforAmmending) {
			if (tweet.label == 1.0) {
				this.posCgood = ((s / (s + 1)) * this.posCgood) + (1 / (s + 1));
				this.posCbad = (s / (s + 1)) * this.posCbad;
			} else {
				this.posCgood = ((s / (s + 1)) * this.posCgood);
				this.posCbad = ((s / (s + 1)) * this.posCbad) + (1 / (s + 1));
			}
		}
		
		setPXc();
	}

	private void setPXc() {
		for (PojoRow tweet : dataforAmmending) {
//			System.out.println("tweet= "+tweet.toString());
			if (tweet.label == 1.0d) {
				this.numberOfCgood = this.numberOfCgood + 1;
				double m = 2 + this.numberOfCgood;
				double[] tweetFeatures = tweet.features.toArray();
				for (int i = 0; i < tweetFeatures.length; i++) {
					if (tweetFeatures[i] == 1d) {
						// for good tweets with 1
						double tmp1 = possibilityOfXEq1givenCgood[i];
						// System.out.println("possibilityOfXEq1givenCgood1=
						// "+possibilityOfXEq1givenCgood[i]+ "i= "+i);
						double newTmp1 = ((m / (m + 1)) * tmp1) + (1 / (m + 1));
						this.possibilityOfXEq1givenCgood[i] = newTmp1;
						double ew = newTmp1;
						/*
						 * System.out.println("possibilityOfXEq1givenCgood= "
						 * +possibilityOfXEq1givenCgood[i]+ "i= "+i);
						 * System.out.println("newTmp1= "+newTmp1);
						 * System.out.println(" ");
						 */

						// for good tweets with 0
						double tmp0 = possibilityOfXEq0givenCgood[i];
						// System.out.println("possibilityOfXEq0givenCgood1=
						// "+possibilityOfXEq0givenCgood[i]+ "i= "+i);
						double newTmp0 = ((m / (m + 1)) * tmp0);
						this.possibilityOfXEq0givenCgood[i] = newTmp0;
						// System.out.println("possibilityOfXEq0givenCgood=
						// "+possibilityOfXEq0givenCgood[i]+ "i= "+i);
						// System.out.println("newTmp0= "+newTmp0);
						// System.out.println(" ");
					}
				}
			} else {
				this.numberOfCbad = this.numberOfCbad + 1;
				double m = 2 + this.numberOfCbad;
				double[] tweetFeatures = tweet.features.toArray();
				for (int i = 0; i < tweetFeatures.length; i++) {
					if (tweetFeatures[i] == 1) {
						// for good tweets with 1
						double tmp1 = possibilityOfX흎1givenCbad[i];
						// System.out.println("possibilityOfX흎1givenCbad1=
						// "+possibilityOfX흎1givenCbad[i]+ "i= "+i);
						double newTmp1 = ((m / (m + 1)) * tmp1) + (1 / (m + 1));
						this.possibilityOfX흎1givenCbad[i] = newTmp1;
						// System.out.println("possibilityOfX흎1givenCbad=
						// "+possibilityOfX흎1givenCbad[i]+ "i= "+i);
						// System.out.println("newTmp1= "+newTmp1);
						// System.out.println(" ");
						// for good tweets with 0
						double tmp0 = possibilityOfX흎0givenCbad[i];
						// System.out.println("possibilityOfX흎0givenCbad1=
						// "+possibilityOfX흎0givenCbad[i]+ "i= "+i);
						double newTmp0 = ((m / (m + 1)) * tmp0);
						this.possibilityOfX흎0givenCbad[i] = newTmp0;
						// System.out.println("possibilityOfX흎0givenCbad=
						// "+possibilityOfX흎0givenCbad[i]+ "i= "+i);
						// System.out.println("newTmp0= "+newTmp0);
						// System.out.println(" ");
					}
				}
			}
			
		}
		System.out.println("possibilityOfXEq1givenCgood= "+Arrays.toString(possibilityOfXEq1givenCgood));
	}

	public double[] getPossibilityOfXEq1givenCgood() {
		return possibilityOfXEq1givenCgood;
	}

	public void setPossibilityOfXEq1givenCgood(double[] possibilityOfXEq1givenCgood) {
		this.possibilityOfXEq1givenCgood = possibilityOfXEq1givenCgood;
	}

	public double[] getPossibilityOfX흎1givenCbad() {
		return possibilityOfX흎1givenCbad;
	}

	public void setPossibilityOfX흎1givenCbad(double[] possibilityOfX흎1givenCbad) {
		this.possibilityOfX흎1givenCbad = possibilityOfX흎1givenCbad;
	}

	public double[] getPossibilityOfXEq0givenCgood() {
		return possibilityOfXEq0givenCgood;
	}

	public void setPossibilityOfXEq0givenCgood(double[] possibilityOfXEq0givenCgood) {
		this.possibilityOfXEq0givenCgood = possibilityOfXEq0givenCgood;
	}

	public double[] getPossibilityOfX흎0givenCbad() {
		return possibilityOfX흎0givenCbad;
	}

	public void setPossibilityOfX흎0givenCbad(double[] possibilityOfX흎0givenCbad) {
		this.possibilityOfX흎0givenCbad = possibilityOfX흎0givenCbad;
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

	@Override
	public String toString() {
		return "AmmendManager [possibilityOfXEq1givenCgood=" + Arrays.toString(possibilityOfXEq1givenCgood)
				+ ", possibilityOfX흎1givenCbad=" + Arrays.toString(possibilityOfX흎1givenCbad) + "]";
	}

}
