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
	

	/*public static void constructNaiveModel(JavaRDD<PojoRow> trainingData) {
		// p(c=1) pithanotita twn kalwn tweets sto set
		// it takes P(c) , P(X|C) as a list of P(x | Ci ) 
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

		final Double probabilityCequalsGoodtweet = goodTweetMap.count() / trainingDataCount;
		
		final Double probabilityCequalsBadTweet = 1 - probabilityCequalsGoodtweet;
		possibilitesOfClases.add(probabilityCequalsGoodtweet);
		possibilitesOfClases.add(probabilityCequalsBadTweet);
		
		// ypologismos twn p(Xi|C)
		Double mapPositiveswnGivenGood = (double) trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[0] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count();
		// Double metritis = (double) katitest.count();
		// Double mapPositiveswnGivenGood = (double)
		// (katitest.count()/goodTweetMap.count());

		NafiBayesModel.possibilityOfXgivenCgood.add(mapPositiveswnGivenGood);
		Double mapPositiveswnGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[0] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapPositiveswnGivenBad);
		// stands for sum of negative labeled tweets

		Double mapNegativeswnGivenGood = (double) trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[1] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count();
		NafiBayesModel.possibilityOfXgivenCgood.add(mapNegativeswnGivenGood);
		Double mapNegativeswnGivenBad = (double) trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[1] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCbad.add(mapNegativeswnGivenBad);

		Double mapSomewhatPositiveswnGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[2] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCgood.add(mapSomewhatPositiveswnGivenGood);

		Double mapSomewhatPositiveswnGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[2] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatPositiveswnGivenBad);

		Double mapNeuturalswnGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[3] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCgood.add(mapNeuturalswnGivenGood);

		Double mapNeuturalswnGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[3] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapNeuturalswnGivenBad);

		Double mapSomewhatNegativeGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[4] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCgood.add(mapSomewhatNegativeGivenGood);

		Double mapSomewhatNegativeGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[4] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatNegativeGivenBad);

		Double mapNegGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[5] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCgood.add(mapNegGivenGood);

		Double mapNegGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[5] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapNegGivenBad);

		Double mapOhSoGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[6] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCgood.add(mapOhSoGivenGood);

		Double mapOhSoGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[6] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapOhSoGivenBad);

		Double mapCapitalGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[7] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCgood.add(mapCapitalGivenGood);

		Double mapCapitalGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[7] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapCapitalGivenBad);

		Double mapIsMetaphorGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[8] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCgood.add(mapIsMetaphorGivenGood);

		Double mapIsMetaphorGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[8] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapIsMetaphorGivenBad);

		Double mapLoveGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[9] == (1.0))
						return true;
				}
				return false;
			}
		}).count() / goodTweetMap.count());
		NafiBayesModel.possibilityOfXgivenCgood.add(mapLoveGivenGood);

		Double mapLoveGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0) {
					double[] temp = v1.getfeatures().toArray();
					if (temp[9] == (0.0))
						return true;
				}
				return false;
			}
		}).count() / (trainingDataCount - goodTweetMap.count()));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapLoveGivenBad);

	}
	public static Double getAccuracyOfNaiveModel(JavaRDD<PojoRow> testData){
		
		JavaPairRDD<Double, Double> predictionAndLabel = testData
				.mapToPair(new PairFunction<PojoRow, Double, Double>() {
					
					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Double> call(PojoRow p) {
						return new Tuple2<Double, Double>(NafiBayesModel.classify(p.getfeatures().toArray(), probabilitiesGeneric.firstKey(),probabilitiesGeneric.lastKey()), p.getLabel());
					}
				});
		double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Double, Double> pl) {
				return pl._1().equals(pl._2());
			}
		}).count() / (double) testData.count();
		return accuracy;
	}
*/
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
	
	
	/*public static void ammendTrainingSetToModel (JavaRDD<PojoRow> testData3){
		double newCountOfTrainingData = countOfTrainingData + testData3.count();
		List<PojoRow> listOfDataForAmmendingInMemmory = testData3.collect();
		Double probabilityCequalsGoodtweetAmmendedbytestData3 = possibilitesOfClases.get(0);
		Double probabilityCequalsBadtweetAmmendedbytestData3 = possibilitesOfClases.get(1);
		for (PojoRow pj : listOfDataForAmmendingInMemmory){
			NafiBayesModel.possibilityOfXgivenCgood.clear();
			NafiBayesModel.possibilityOfXgivenCbad.clear();

			if (pj.label==1.0){
				probabilityCequalsGoodtweetAmmendedbytestData3 = 
						((double)newCountOfTrainingData/((double)newCountOfTrainingData +1 ) )*
						probabilityCequalsGoodtweetAmmendedbytestData3 + 
						(double)(1/(newCountOfTrainingData+1));
				
				
				probabilityCequalsBadtweetAmmendedbytestData3 = 
						(double)((double)newCountOfTrainingData/((double)newCountOfTrainingData +1 ) )*
						(double)probabilityCequalsBadtweetAmmendedbytestData3;
				if (pj.features.toArray()[0]== 1.00){
					swn_positive, 
						swn_negative,
						swn_somewhatPositive,
						swn_neutural,
						swn_somewhatNegative,
						neg,
						ohso,
						capital,
						isMetaphor,
						love
					mapPositiveswnGivenGood = ((double)mapPositiveswnGivenGood*(m/(m + 1))+(1/(1+m)));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapPositiveswnGivenGood);
				} else {
					mapPositiveswnGivenGood = ((double)mapPositiveswnGivenGood*(m/(m + 1)));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapPositiveswnGivenGood);

				}if (pj.features.toArray()[1]== 1.00){
					mapNegativeswnGivenGood = ((double)mapNegativeswnGivenGood*(m/(m + 1))+(1/(1+m)));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapNegativeswnGivenGood);

				} else {
					mapNegativeswnGivenGood = ((double)mapNegativeswnGivenGood*(m/(m + 1)));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapNegativeswnGivenGood);

				}if (pj.features.toArray()[2]== 1.00){
					mapSomewhatPositiveswnGivenGood= (double)mapSomewhatPositiveswnGivenGood*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapSomewhatPositiveswnGivenGood);

				} else {
					mapSomewhatPositiveswnGivenGood= (double)mapSomewhatPositiveswnGivenGood*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapSomewhatPositiveswnGivenGood);


				}if (pj.features.toArray()[3]== 1.00){
					mapNeuturalswnGivenGood = (double)mapNeuturalswnGivenGood*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapNeuturalswnGivenGood);

				} else {
					mapNeuturalswnGivenGood= (double)mapNeuturalswnGivenGood*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapNeuturalswnGivenGood);

				}if (pj.features.toArray()[4]== 1.00){
					mapSomewhatNegativeGivenGood = (double)mapSomewhatNegativeGivenGood*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapSomewhatNegativeGivenGood);

				} else {
					mapSomewhatNegativeGivenGood = (double)mapSomewhatNegativeGivenGood*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapSomewhatNegativeGivenGood);

				}if (pj.features.toArray()[5]== 1.00){
					mapNegGivenGood =  (double)mapNegGivenGood*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapNegGivenGood);

				} else {
					mapNegGivenGood =  (double)mapNegGivenGood*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapNegGivenGood);

				}if (pj.features.toArray()[6]== 1.00){
					mapOhSoGivenGood=  (double)mapOhSoGivenGood*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapOhSoGivenGood);

				} else {
					mapOhSoGivenGood=  (double)mapOhSoGivenGood*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapOhSoGivenGood);

				}if (pj.features.toArray()[7]== 1.00){
					mapCapitalGivenGood = (double)mapCapitalGivenGood*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapCapitalGivenGood);

				} else {
					mapCapitalGivenGood = (double)mapCapitalGivenGood*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapCapitalGivenGood);

				}if (pj.features.toArray()[8]== 1.00){
					mapIsMetaphorGivenGood= (double)mapIsMetaphorGivenGood*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapIsMetaphorGivenGood);

				} else {
					mapIsMetaphorGivenGood= (double)mapIsMetaphorGivenGood*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapIsMetaphorGivenGood);

					
				}if (pj.features.toArray()[9]== 1.00){
					mapLoveGivenGood= (double)mapLoveGivenGood*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapLoveGivenGood);
				} else {
					mapLoveGivenGood= (double)mapLoveGivenGood*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCgood.add(mapLoveGivenGood);
				}
				
			}else{
				probabilityCequalsBadtweetAmmendedbytestData3 = 
						(double)(newCountOfTrainingData/((double)newCountOfTrainingData +1 ) )*
						probabilityCequalsBadtweetAmmendedbytestData3+(double)(1/((double)newCountOfTrainingData+1));
				probabilityCequalsGoodtweetAmmendedbytestData3 = 
						(double)((double)newCountOfTrainingData/((double)newCountOfTrainingData +1 ) )*
						probabilityCequalsGoodtweetAmmendedbytestData3;
				
				if (pj.features.toArray()[0]== 0.00){
					swn_positive, 
						swn_negative,
						swn_somewhatPositive,
						swn_neutural,
						swn_somewhatNegative,
						neg,
						ohso,
						capital,
						isMetaphor,
						love
					mapPositiveswnGivenBad = ((double)mapPositiveswnGivenBad*(m/(m + 1))+(1/(1+m)));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapPositiveswnGivenBad);

					
				} else {
					mapPositiveswnGivenBad = ((double)mapPositiveswnGivenBad*(m/(m + 1)));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapPositiveswnGivenBad);
					
				}if (pj.features.toArray()[1]== 0.00){
					mapNegativeswnGivenBad = ((double)mapNegativeswnGivenBad*(m/(m + 1))+(1/(1+m)));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNegativeswnGivenBad);
				} else {
					mapNegativeswnGivenBad = ((double)mapNegativeswnGivenBad*(m/(m + 1)));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNegativeswnGivenBad);

					
				}if (pj.features.toArray()[2]== 0.00){
					mapSomewhatPositiveswnGivenBad= (double)mapSomewhatPositiveswnGivenBad*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatPositiveswnGivenBad);

				} else {
					mapSomewhatPositiveswnGivenBad= (double)mapSomewhatPositiveswnGivenBad*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatPositiveswnGivenBad);


				}if (pj.features.toArray()[3]== 0.00){
					mapNeuturalswnGivenBad = (double)mapNeuturalswnGivenBad*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNeuturalswnGivenBad);

				} else {
					mapNeuturalswnGivenBad= (double)mapNeuturalswnGivenBad*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNeuturalswnGivenBad);

				}if (pj.features.toArray()[4]== 0.00){
					mapSomewhatNegativeGivenBad = (double)mapSomewhatNegativeGivenBad*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatNegativeGivenBad);

				} else {
					mapSomewhatNegativeGivenBad = (double)mapSomewhatNegativeGivenBad*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatNegativeGivenBad);

				}if (pj.features.toArray()[5]== 0.00){
					mapNegGivenBad =  (double)mapNegGivenBad*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNegGivenBad);

				} else {
					mapNegGivenBad =  (double)mapNegGivenBad*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNegGivenBad);

				}if (pj.features.toArray()[6]== 0.00){
					mapOhSoGivenBad=  (double)mapOhSoGivenBad*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapOhSoGivenBad);

				} else {
					mapOhSoGivenBad=  (double)mapOhSoGivenBad*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapOhSoGivenBad);

				}if (pj.features.toArray()[7]== 0.00){
					mapCapitalGivenBad = (double)mapCapitalGivenBad*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapCapitalGivenBad);

				} else {
					mapCapitalGivenBad = (double)mapCapitalGivenBad*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapCapitalGivenBad);

				}if (pj.features.toArray()[8]== 0.00){
					mapIsMetaphorGivenBad= (double)mapIsMetaphorGivenBad*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapIsMetaphorGivenBad);


				} else {
					mapIsMetaphorGivenBad= (double)mapIsMetaphorGivenBad*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapIsMetaphorGivenBad);

					
				}if (pj.features.toArray()[9]== 0.00){
					mapLoveGivenBad= (double)mapLoveGivenBad*(m/(m + 1))+(1/(1+m));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapLoveGivenBad);


				} else {
					mapLoveGivenBad= (double)mapLoveGivenBad*(m/(m + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapLoveGivenBad);

					
				}
			
			
			
			}
		}
	}*/

}
