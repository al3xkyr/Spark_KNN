package org.sparkexample.classifiers1;

import java.util.ArrayList;
import java.util.Vector;

public class NafiBayesModel {
	public static Vector<Double> possibilityOfXgivenCgood = new Vector<Double>();
	public static Vector<Double> possibilityOfXgivenCbad = new Vector<Double>();
		// edw ginetai to classification
	/**
	 * As it discribed here on Bernouli NaiveBayes model
	 * http://www.inf.ed.ac.uk/teaching/courses/inf2b/learnnotes/inf2b-learn-note07-2up.pdf
	 * @param featureForClassification
	 * @param possibilityOfCgood
	 * @param possibilityOfCbad
	 * @return
	 */
	public static Double classify(double[] featureForClassification, double possibilityOfCgood, double possibilityOfCbad) {

		Double possibilityOfCGoodGivenX = 1.0;
		Double possibilityOfCBadGivenX = 1.0;

		for (int i = 0; i < featureForClassification.length; i++) {
			possibilityOfCGoodGivenX *= (featureForClassification[i] * possibilityOfXgivenCgood.get(i))
					+ ((1 - featureForClassification[i]) * (1 - possibilityOfXgivenCgood.get(i)));
			possibilityOfCBadGivenX *= (featureForClassification[i] * possibilityOfXgivenCbad.get(i))
					+ ((1 - featureForClassification[i]) * (1 - possibilityOfXgivenCbad.get(i)));

		}
		if ((possibilityOfCgood * possibilityOfCGoodGivenX) > (possibilityOfCBadGivenX * possibilityOfCbad)) {
			return (double) 1;
		}
		return (double) 0;

	}
	

}
