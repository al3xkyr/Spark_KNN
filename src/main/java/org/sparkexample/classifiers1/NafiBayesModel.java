package org.sparkexample.classifiers1;

import java.util.ArrayList;
import java.util.Vector;

public class NafiBayesModel {
	public static Vector<Double> possibilityOfXgivenCgood = new Vector<Double>();
	public static Vector<Double> possibilityOfXgivenCbad = new Vector<Double>();
		// edw ginetai to classification
	public static Double classify(double[] ds, double possibilityOfCgood, double possibilityOfCbad) {

		Double possibilityOfCGoodGivenX = 1.0;
		Double possibilityOfCBadGivenX = 1.0;

		for (int i = 0; i < ds.length; i++) {
			possibilityOfCGoodGivenX *= (ds[i] * possibilityOfXgivenCgood.get(i))
					+ ((1 - ds[i]) * (1 - possibilityOfXgivenCgood.get(i)));
			possibilityOfCBadGivenX *= (ds[i] * possibilityOfXgivenCbad.get(i))
					+ ((1 - ds[i]) * (1 - possibilityOfXgivenCbad.get(i)));

		}
		if ((possibilityOfCgood * possibilityOfCGoodGivenX) > (possibilityOfCBadGivenX * possibilityOfCbad)) {
			return (double) 1;
		}
		return (double) 0;

	}
	

}
