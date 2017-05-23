package org.sparkexample.classifiers1;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.sparkexample.pojo1.PojoRow;

import scala.Tuple2;

public class NafiBayesModel {
	public static ArrayList<Double> possibilityOfXgivenCgood = new ArrayList<Double>();
	public static ArrayList<Double> possibilityOfXgivenCbad = new ArrayList<Double>();
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
