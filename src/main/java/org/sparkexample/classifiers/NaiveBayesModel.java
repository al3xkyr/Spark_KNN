package org.sparkexample.classifiers;

public class NaiveBayesModel {

	public static double calculatePxgivenC (double[] possibilityOfXEq1,
			double[] possibilityOfXEq0 ,	double[] featureForClassification)
	{
		double pXgivenC = 1.0 ; 
		for ( int i = 0 ; i < featureForClassification.length ; i++ ){
			if ((featureForClassification[i] == 1.0)){
				pXgivenC *= possibilityOfXEq1[i];
			}else { 
				pXgivenC *= possibilityOfXEq0[i];
			}
		}
		return pXgivenC;
		
	} 
	 
	public static double classify(
			double[] possibilityOfXEq1givenCgood, 
			double[] possibilityOfXEq0givenCgood,
			double[] possibilityOfXEq1givenCbad,
			double[] possibilityOfXEq0givenCbad,
			double possibilityOfCgood, 
			double possibilityOfCbad,
			double[] featureForClassification) {
		double pXgivenCgood = calculatePxgivenC(possibilityOfXEq1givenCgood, possibilityOfXEq0givenCgood, featureForClassification);
		double pXgivenCbad = calculatePxgivenC(possibilityOfXEq1givenCbad, possibilityOfXEq0givenCbad, featureForClassification);
		if ((pXgivenCgood * possibilityOfCgood) > (pXgivenCbad * possibilityOfCbad)) {
			return (double) 1;
		}
		return (double) 0;

	}

}
