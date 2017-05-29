package org.sparkexample.classifiers1;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.classification.KNNClassificationModel;
import org.apache.spark.ml.classification.KNNClassifier;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.sparkexample.pojo1.DataFeature;
import org.sparkexample.pojo1.PojoRow;

import scala.Tuple2;

public class CustomNaiveBayes {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("org.sparkexample.WordCount2").setMaster("local[*]");
		/*@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(sparkConf);*/
		SparkContext sc1 = new SparkContext(sparkConf) ;
		@SuppressWarnings("resource")
		SparkSession sqlSpark = new SparkSession(sc1);
		
		

		// Load and parse the data file.
		String datapath = "data/60000tweets.csv";
		
		JavaRDD<String> dataAsStrings = sc1.textFile(datapath, 5).toJavaRDD();
		// transformation on dataset to be cleaned and create the datafeature
		// which has swn score and the attributes that i want to store
		// pernei to rdd apo panw kai to kanei apo RDD string se RDD apo
		// dataFeature
		
		JavaRDD<DataFeature> cleaned = dataAsStrings.map(new Function<String, DataFeature>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public DataFeature call(String s) throws Exception {
				DataFeature dataFeature = new DataFeature();

				String json2 = s.substring(1, s.lastIndexOf("}") + 1);

				String result = s.substring(s.length() - 3, s.length()).replace(",", "").replace("\"", "");
				int lastrow = Integer.valueOf(result);
				double lable;
				if (lastrow == -1) {
					lable = 0;
				} else {
					lable = 1;
				}
				JSONObject json1 = null;
				Double swn = null;
				try {
					json1 = new JSONObject(json2);
					swn = json1.getDouble("__swn_score__");
					dataFeature.setData(json1);
					dataFeature.setLable(lable);
				} catch (JSONException e) {
					e.printStackTrace();
					return null;

				}

				return dataFeature;
			}
		}).cache(); // edw ta krataei stin mnimi

		JavaRDD<PojoRow> labeledPointJavaRDD = cleaned.map(new Function<DataFeature, PojoRow>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public PojoRow call(DataFeature dataFeature) throws Exception {

				double neg;
				boolean negation = dataFeature.getData().getBoolean("__NEGATION__");
				if (negation) {
					neg = 1;
				} else {
					neg = 0;
				}
				double negSm;
				boolean negSmile = dataFeature.getData().getBoolean("__NEG_SMILEY__");
				if (negSmile) {
					negSm = 1;
				} else {
					negSm = 0;
				}

				double posSm;
				boolean pos = dataFeature.getData().getBoolean("__POS_SMILEY__");
				if (pos) {
					posSm = 1;

				} else {
					posSm = 0;
				}
				double ohso;
				boolean ohsoBool = dataFeature.getData().getBoolean("__OH_SO__");
				if (ohsoBool) {
					ohso = 1;

				} else {
					ohso = 0;
				}
				double capital;
				boolean capitalBool = dataFeature.getData().getBoolean("__CAPITAL__");
				if (capitalBool) {
					capital = 1;

				} else {
					capital = 0;
				}
				double isMetaphor;
				boolean isMetaphorBool = dataFeature.getData().getBoolean("__is_metaphor__");
				if (isMetaphorBool){
					isMetaphor =1 ;
				}else {
					isMetaphor =0 ; 
				}
				// ,neg,ohso,capital
				double love;
				boolean loveBool = dataFeature.getData().getBoolean("__LOVE__");
				if (loveBool ){
					love = 1 ; 
				}else {
					love = 0;
				}
				// breaking the double value for total swn score based on the 
				// observations about the polarity of the tweet by means 
				double swn_positive = 0 ;
				double swn_negative=0;
				double swn_somewhatPositive= 0;
				double swn_neutural= 0 ; 
				double swn_somewhatNegative= 0;
				double swnScore = dataFeature.getData().getDouble("__swn_score__");
				if (swnScore>1.2){
					swn_positive = 1;
				} else if (swnScore <= 1.2 && swnScore > 0.95){
					swn_somewhatPositive = 1;
				} else if (swnScore <= 0.95 && swnScore >0.5 ){
					swn_neutural = 1;
				} else if (swnScore <= 0.5 && swnScore > 0.2){
					swn_somewhatNegative = 1 ;
				} else if ( swnScore < 0.2){
					swn_negative = 1;
				}
				// epilegontai features pou den einai eutheos analoga me ton prosdiorismo tis klasis tou tweet 
				// diladi ta positive smileys kai ta negatives einai amesa sindedemena me tin akrivia tou classifier 
				double[] arrayofVector = {
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
						};
				Vector dv = Vectors.dense(arrayofVector);
				PojoRow rowPojoforProcess = new PojoRow(dataFeature.getLable(), dv);
				return rowPojoforProcess;
			}
		});
		


		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<PojoRow>[] splits = labeledPointJavaRDD.randomSplit(new double[] { 0.5, 0.1, 0.1, 0.3 });
		
		JavaRDD<PojoRow> trainingData = splits[0];
		JavaRDD<PojoRow> testData = splits[1]; // arxiki morfi , meta allaxtikan apo ton knn me traindataCollectedwithLabelFromKNNFromTestData san lista
		
		JavaRDD<PojoRow> testDataForValidation = splits[2];
		JavaRDD<PojoRow> trainingData2 = splits[3];
		
		
		
		Dataset<Row> labeledPointDataset = sqlSpark.createDataFrame(trainingData , PojoRow.class);
		
		// Knn classification
		KNNClassifier val = new KNNClassifier()
				  .setTopTreeSize(2)
				  .setK(5);
		val.train(labeledPointDataset);
		
		KNNClassificationModel model = val.fit(labeledPointDataset);
		val.transform(labeledPointDataset, model.topTree(), model.subTrees());
		Dataset<Row> labeledPointDatasetforTest = sqlSpark.createDataFrame(testData , PojoRow.class);
		RDD<Tuple2<Object, Tuple2<Row, Object>[]>> temp = val.transform(labeledPointDatasetforTest, model.topTree(), model.subTrees());
		
		
		
		Dataset<Row> sss = model.transform(labeledPointDatasetforTest);
		sss.show();
		Dataset<Row> j32 = sss.select("features" , "label", "prediction");
		j32.show();
		JavaPairRDD<Double , Double> ewo = j32.select("prediction", "label").toJavaRDD().mapToPair(new PairFunction<Row, Double, Double>() {

			public Tuple2<Double, Double> call(Row t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Double, Double>(t.getDouble(0), t.getDouble(1));
			}
		});
		double accuracyOfKNNOnTestData = ewo.filter(new Function<Tuple2<Double,Double>, Boolean>() {
			
			public Boolean call(Tuple2<Double, Double> v1) throws Exception {
				// TODO Auto-generated method stub
				if (v1._1.doubleValue()==v1._2.doubleValue()){
				return true;}
				return false;
			}
		}).count()/(double)(testData.count());
		System.out.println("the accuaracy of knn is "+ accuracyOfKNNOnTestData);
		
		//creating new rdd with label the prediction on test data 
		JavaRDD<PojoRow> trainDataForNaiveWithPredictionsOfKNN = j32.select("prediction", "features").toJavaRDD().map(new Function<Row, PojoRow>() {

			public PojoRow call(Row t) throws Exception {
				// TODO Auto-generated method stub
				return new PojoRow((Double) t.get(0), (Vector) t.get(1));
			}
		});
		List<PojoRow> traindataCollectedwithLabelFromKNNFromTestData = trainDataForNaiveWithPredictionsOfKNN.collect();
		
		// end of knn 
		
		// have to put in type of  < Integer , PojoRow > where 
		// Integer is the nth element of feature array Pojorow contains the array 
		
		
		
		// In order to calculate model of naive bayes 
		// we want to calculate
		// P(C|X) = P(X|C)*P(C) / P(X) for each new tweet for each class ( good , bad )
		// On given tweet that has the biggest posterior probability on a class assigned there
		// Naive model 
		
		double trainingDataCount = trainingData.count();
		JavaRDD<PojoRow> goodTweetMap = trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					return true ;
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

		// p(c=1) pithanotita twn kalwn tweets sto set
		long goodTweetMapNumber = goodTweetMap.count();
		long badTweetMapNumber = badTweetMap.count();
		final Double probabilityCequalsGoodtweet = goodTweetMap.count()/trainingDataCount;
		final Double probabilityCequalsBadTweet = 1 - probabilityCequalsGoodtweet;
		
		// In order to decouple algorithm from the specific features 
		// i have to hold it on an array of 
		PojoRow pojoOfSumOfGoodTweets = goodTweetMap.reduce(new Function2<PojoRow, PojoRow, PojoRow>() {
			
			public PojoRow call(PojoRow v1, PojoRow v2) throws Exception {
				// TODO Auto-generated method stub
				double[] v1Array = v1.features.toArray();
				double[] v2Array = v2.features.toArray();
				double[] vSumArray = new double[v1.features.size()] ;
				for (int i = 0 ; i < v1Array.length ; i ++){
					vSumArray[i]= v1Array[i]+v2Array[i];
				}
				return 
				new PojoRow(v1.label, Vectors.dense(vSumArray));
			}
		});
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
		PojoRow pojoOfProbabilitiesOfGoodTweets = addPosibilitiesBasedOnSumsforGoodTweets(pojoOfSumOfGoodTweets , goodTweetMapNumber);
		PojoRow pojoOfProbabilitiesOfBadTweets = addPosibilitiesBasedOnSumsforBadTweets(pojoOfSumOfBadTweets , badTweetMapNumber);
		
		
 		// ypologismos twn p(Xi|C)
		Double mapPositiveswnGivenGood = (double) trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[0]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber;
		//Double metritis = (double) katitest.count();
		//Double mapPositiveswnGivenGood = (double) (katitest.count()/goodTweetMapNumber);
		
		NafiBayesModel.possibilityOfXgivenCgood.add( mapPositiveswnGivenGood);
		Double mapPositiveswnGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[0]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapPositiveswnGivenBad);
		// stands for sum of negative labeled tweets
		
		
		Double mapNegativeswnGivenGood = (double) trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[1]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber;
		NafiBayesModel.possibilityOfXgivenCgood.add(mapNegativeswnGivenGood);		
		Double mapNegativeswnGivenBad = (double) trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[1]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCbad.add(mapNegativeswnGivenBad);
		
		Double mapSomewhatPositiveswnGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[2]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCgood.add(mapSomewhatPositiveswnGivenGood);		

		

		Double mapSomewhatPositiveswnGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[2]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatPositiveswnGivenBad);

		
		
		Double mapNeuturalswnGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[3]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCgood.add(mapNeuturalswnGivenGood);		


		Double mapNeuturalswnGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[3]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapNeuturalswnGivenBad);

		
		Double mapSomewhatNegativeGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[4]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCgood.add(mapSomewhatNegativeGivenGood);		

		Double mapSomewhatNegativeGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[4]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatNegativeGivenBad);

		
		Double mapNegGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[5]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCgood.add(mapNegGivenGood);		

		
		Double mapNegGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[5]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapNegGivenBad);

		
		Double mapOhSoGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[6]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCgood.add(mapOhSoGivenGood);		

		
		Double mapOhSoGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[6]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapOhSoGivenBad);

		Double mapCapitalGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
		
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[7]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCgood.add(mapCapitalGivenGood);		

		Double mapCapitalGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[7]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapCapitalGivenBad);

		
		Double mapIsMetaphorGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[8]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCgood.add(mapIsMetaphorGivenGood);		

		Double mapIsMetaphorGivenBad= (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[8]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapIsMetaphorGivenBad);

		Double mapLoveGivenGood = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
		
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 1.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[9]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/goodTweetMapNumber);
		NafiBayesModel.possibilityOfXgivenCgood.add(mapLoveGivenGood);		

		
		Double mapLoveGivenBad = (double) (trainingData.filter(new Function<PojoRow, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if (v1.getLabel() == 0.0 ){
					double[] temp = v1.getfeatures().toArray();
					if(temp[9]== (1.0))
					return true ;
				}
				return false;
			}
			}).count()/(trainingDataCount - goodTweetMapNumber));
		NafiBayesModel.possibilityOfXgivenCbad.add(mapLoveGivenBad);

		// --------------end of sums ---------------and probabilities 
		JavaPairRDD<Double, Double> predictionAndLabelForTestDataForValidationwithOldModel = testDataForValidation
				.mapToPair(new PairFunction<PojoRow, Double, Double>() {
					
					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Double> call(PojoRow p) {
						return new Tuple2<Double, Double>(NafiBayesModel.classify(p.getfeatures().toArray(), probabilityCequalsGoodtweet ,probabilityCequalsBadTweet ), p.getLabel());
					}
				});
		double acccuracyOnTestDataForValidationWithOldModel = predictionAndLabelForTestDataForValidationwithOldModel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Double, Double> pl) {
				return pl._1().equals(pl._2());
			}
		}).count() / (double) testDataForValidation.count();
		
		
		
		
		
		//	AMMEND test set to model 
		// calculating the count of combined training model 
		 double newCountOfTrainingData = trainingData.count()+traindataCollectedwithLabelFromKNNFromTestData.size();
		//testData3 is the dataset for ammending 
		//Double ammentedProbabilityCequalsGoodtweet = testData3.
		
		// classification me ammending
		double probabilityCequalsGoodtweetAmmendedbytestDatafromKNN =  probabilityCequalsGoodtweet;
		double probabilityCequalsBadtweetAmmendedbytestDatafromKNN = probabilityCequalsBadTweet;
		
		// lack of parametizing global variable through nodes have to get dataset to master in order to 
		// modify the p(c) and p(x|c) accordingly
		double m = (double)goodTweetMapNumber+ 2 ;
		double m2 = (double)(trainingDataCount-goodTweetMapNumber)+2 ;
		


		for (PojoRow pj : traindataCollectedwithLabelFromKNNFromTestData){

			if (pj.label==1.0){
				NafiBayesModel.possibilityOfXgivenCgood.clear();

				probabilityCequalsGoodtweetAmmendedbytestDatafromKNN = 
						(((double)newCountOfTrainingData/((double)newCountOfTrainingData +1 ) )*
						probabilityCequalsGoodtweetAmmendedbytestDatafromKNN) + 
						(double)(1/(newCountOfTrainingData+1));
				
				
				probabilityCequalsBadtweetAmmendedbytestDatafromKNN = 
						(double)((double)newCountOfTrainingData/((double)newCountOfTrainingData +1 ) )*
						(double)probabilityCequalsBadtweetAmmendedbytestDatafromKNN;
				if (pj.features.toArray()[0]== 1.00){
					/*swn_positive, 
						swn_negative,
						swn_somewhatPositive,
						swn_neutural,
						swn_somewhatNegative,
						neg,
						ohso,
						capital,
						isMetaphor,
						love*/
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
				NafiBayesModel.possibilityOfXgivenCbad.clear();

				probabilityCequalsBadtweetAmmendedbytestDatafromKNN = 
						(double)(newCountOfTrainingData/((double)newCountOfTrainingData +1 ) )*
						probabilityCequalsBadtweetAmmendedbytestDatafromKNN+(double)(1/((double)newCountOfTrainingData+1));
				probabilityCequalsGoodtweetAmmendedbytestDatafromKNN = 
						(double)((double)newCountOfTrainingData/((double)newCountOfTrainingData +1 ) )*
						probabilityCequalsGoodtweetAmmendedbytestDatafromKNN;
				
				if (pj.features.toArray()[0]== 0.00){
					/*swn_positive, 
						swn_negative,
						swn_somewhatPositive,
						swn_neutural,
						swn_somewhatNegative,
						neg,
						ohso,
						capital,
						isMetaphor,
						love*/
					mapPositiveswnGivenBad = ((double)mapPositiveswnGivenBad*(m2/(m2 + 1))+(1/(1+m2)));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapPositiveswnGivenBad);

					
				} else {
					mapPositiveswnGivenBad = ((double)mapPositiveswnGivenBad*(m2/(m2 + 1)));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapPositiveswnGivenBad);
					
				}if (pj.features.toArray()[1]== 0.00){
					mapNegativeswnGivenBad = ((double)mapNegativeswnGivenBad*(m2/(m2 + 1))+(1/(1+m2)));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNegativeswnGivenBad);
				} else {
					mapNegativeswnGivenBad = ((double)mapNegativeswnGivenBad*(m2/(m2 + 1)));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNegativeswnGivenBad);

					
				}if (pj.features.toArray()[2]== 0.00){
					mapSomewhatPositiveswnGivenBad= (double)mapSomewhatPositiveswnGivenBad*(m2/(m2 + 1))+(1/(1+m2));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatPositiveswnGivenBad);

				} else {
					mapSomewhatPositiveswnGivenBad= (double)mapSomewhatPositiveswnGivenBad*(m2/(m2 + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatPositiveswnGivenBad);


				}if (pj.features.toArray()[3]== 0.00){
					mapNeuturalswnGivenBad = (double)mapNeuturalswnGivenBad*(m2/(m2 + 1))+(1/(1+m2));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNeuturalswnGivenBad);

				} else {
					mapNeuturalswnGivenBad= (double)mapNeuturalswnGivenBad*(m2/(m2 + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNeuturalswnGivenBad);

				}if (pj.features.toArray()[4]== 0.00){
					mapSomewhatNegativeGivenBad = (double)mapSomewhatNegativeGivenBad*(m2/(m2 + 1))+(1/(1+m2));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatNegativeGivenBad);

				} else {
					mapSomewhatNegativeGivenBad = (double)mapSomewhatNegativeGivenBad*(m2/(m2 + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapSomewhatNegativeGivenBad);

				}if (pj.features.toArray()[5]== 0.00){
					mapNegGivenBad =  (double)mapNegGivenBad*(m2/(m2 + 1))+(1/(1+m2));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNegGivenBad);

				} else {
					mapNegGivenBad =  (double)mapNegGivenBad*(m2/(m2 + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapNegGivenBad);

				}if (pj.features.toArray()[6]== 0.00){
					mapOhSoGivenBad=  (double)mapOhSoGivenBad*(m2/(m2 + 1))+(1/(1+m2));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapOhSoGivenBad);

				} else {
					mapOhSoGivenBad=  (double)mapOhSoGivenBad*(m2/(m2 + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapOhSoGivenBad);

				}if (pj.features.toArray()[7]== 0.00){
					mapCapitalGivenBad = (double)mapCapitalGivenBad*(m2/(m2 + 1))+(1/(1+m2));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapCapitalGivenBad);

				} else {
					mapCapitalGivenBad = (double)mapCapitalGivenBad*(m2/(m2 + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapCapitalGivenBad);

				}if (pj.features.toArray()[8]== 0.00){
					mapIsMetaphorGivenBad= (double)mapIsMetaphorGivenBad*(m2/(m2 + 1))+(1/(1+m2));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapIsMetaphorGivenBad);


				} else {
					mapIsMetaphorGivenBad= (double)mapIsMetaphorGivenBad*(m2/(m2 + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapIsMetaphorGivenBad);

					
				}if (pj.features.toArray()[9]== 0.00){
					mapLoveGivenBad= (double)mapLoveGivenBad*(m2/(m2 + 1))+(1/(1+m2));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapLoveGivenBad);


				} else {
					mapLoveGivenBad= (double)mapLoveGivenBad*(m2/(m2 + 1));
					NafiBayesModel.possibilityOfXgivenCbad.add(mapLoveGivenBad);

					
				}
			
			
			
			}
		}
		final double probabilityCequalsGoodtweetAmmendedbytestDatafromKNNfinal = probabilityCequalsGoodtweetAmmendedbytestDatafromKNN;
		final double probabilityCequalsBadtweetAmmendedbytestDatafromKNNfinal = probabilityCequalsBadtweetAmmendedbytestDatafromKNN;
		
		JavaPairRDD<Double, Double> predictionAndLabelForTestDataforValidationwithNewModel = testDataForValidation
				.mapToPair(new PairFunction<PojoRow, Double, Double>() {
					
					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Double> call(PojoRow p) {
						return new Tuple2<Double, Double>(NafiBayesModel.classify(p.getfeatures().toArray(), probabilityCequalsGoodtweetAmmendedbytestDatafromKNNfinal ,probabilityCequalsBadtweetAmmendedbytestDatafromKNNfinal ), p.getLabel());
					}
				});
		double accuracyonTestDataForValidationwithNewModel = predictionAndLabelForTestDataforValidationwithNewModel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Double, Double> pl) {
				return pl._1().equals(pl._2());
			}
		}).count() / (double) testDataForValidation.count();
		
		System.out.println("The accuracy of Old naive model with dataForValidation is "+ acccuracyOnTestDataForValidationWithOldModel);
		System.out.println("The accuracy of new naive model with dataForValidation is "+ accuracyonTestDataForValidationwithNewModel);

		// end of ammend 
		
		
		
		
		
	}
	
	/**
	 Add to a PojoRow the posibilities of each feature / count of features that have bad label 
	 * add to NafiBayesModel those features
	 * @param pojoOfSumOfBadTweets
	 * @param badTweetMapNumber
	 * @return
	 */

	private static PojoRow addPosibilitiesBasedOnSumsforBadTweets(PojoRow pojoOfSumOfBadTweets, long badTweetMapNumber) {
		// TODO Auto-generated method stub
				double[]  arrayOfFeatures = pojoOfSumOfBadTweets.features.toArray();
				double[]  arrayOfProbabilitites = new double [arrayOfFeatures.length];
				NafiBayesModel.possibilityOfXgivenCbad.clear();
				for ( int i = 0 ; i < arrayOfFeatures.length ; i ++){
					arrayOfProbabilitites[i] = arrayOfFeatures[i]/badTweetMapNumber;
					NafiBayesModel.possibilityOfXgivenCbad.add(arrayOfFeatures[i]/badTweetMapNumber);
				}		
				return new PojoRow(pojoOfSumOfBadTweets.label, Vectors.dense(arrayOfProbabilitites));
	}


	/**
	 * Add to a PojoRow the posibilities of each feature / count of features that have good label 
	 * add to NafiBayesModel those features
	 * @param pojoOfSumOfGoodTweets
	 * @param goodTweetMapNumber
	 * @return
	 */
	private static PojoRow addPosibilitiesBasedOnSumsforGoodTweets(PojoRow pojoOfSumOfGoodTweets, long goodTweetMapNumber) {
		// TODO Auto-generated method stub
		double[]  arrayOfFeatures = pojoOfSumOfGoodTweets.features.toArray();
		double[]  arrayOfProbabilitites = new double [arrayOfFeatures.length];
		NafiBayesModel.possibilityOfXgivenCgood.clear();
		for ( int i = 0 ; i < arrayOfFeatures.length ; i ++){
			arrayOfProbabilitites[i] = arrayOfFeatures[i]/goodTweetMapNumber;
			NafiBayesModel.possibilityOfXgivenCgood.add(arrayOfFeatures[i]/goodTweetMapNumber);
		}
		
		
		return new PojoRow(pojoOfSumOfGoodTweets.label, Vectors.dense(arrayOfProbabilitites));
	}



	public static boolean doesModelNeedRetrain(JavaRDD<PojoRow> trainingData ,JavaRDD<PojoRow> testData ){
		if((getEntropyOfADataSet(testData) - getEntropyOfADataSet(trainingData)) < 0.1 
				|| (getEntropyOfADataSet(testData) - getEntropyOfADataSet(trainingData)) > -0.1 ){
			return true;
		}
		return false ;
	}
	
	
	
	public static double getEntropyOfADataSet(JavaRDD<PojoRow> trainingData){
		// entropy of whole data set 
        // change from labeledPointJavaRDD to testdata
        JavaRDD<PojoRow> rddOfPositiveTweets = trainingData.filter(new Function<PojoRow, Boolean>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(PojoRow v1) throws Exception {
				if(v1.getLabel() == 1.0){
				return true;
				}return false;
			}
		});
		long numOfNegativeTweets = trainingData.count() - rddOfPositiveTweets.count();
		Double possibilityOfPositive = Double.longBitsToDouble(rddOfPositiveTweets.count())/Double.longBitsToDouble(trainingData.count());
		Double possibilityOfNegative = Double.longBitsToDouble(numOfNegativeTweets)/Double.longBitsToDouble(trainingData.count());
		Double entropyOfDataset =-(
				(possibilityOfPositive)
				*(Math.log(possibilityOfPositive)/Math.log(2))
				+ ((possibilityOfNegative)
				*(Math.log(possibilityOfNegative)/Math.log(2))));
		return entropyOfDataset;
		
	}
}
