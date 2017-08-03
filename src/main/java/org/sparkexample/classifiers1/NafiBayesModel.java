package org.sparkexample.classifiers1;

import java.util.ArrayList;
import java.util.Vector;

public class NafiBayesModel {

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
	
	public static double classify(double pXgivenCgood ,double pXgivenCbad, double possibilityOfCgood, double possibilityOfCbad ) {

		if ((pXgivenCgood * possibilityOfCgood) > (pXgivenCbad * possibilityOfCbad)) {
			return (double) 1;
		}
		return (double) 0;

	}

}
