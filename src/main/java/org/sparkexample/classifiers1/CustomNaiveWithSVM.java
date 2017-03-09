package org.sparkexample.classifiers1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.sparkexample.pojo1.DataFeature;

import scala.Tuple2;

public class CustomNaiveWithSVM {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        // Load and parse the data file.
        //String datapath = "/home/pvendras/LIBSVM2.txt";
        //        String datapathPanos = "/home/pvendras/LIBSVM2.txt";
        String datapath = "data/60000tweets.csv";
//        String datapathPanos = "/home/pvendras/featuresTest.txt";
//        JavaRDD<LabeledPoint> trainingData = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD();


        JavaRDD<String> dataRDD = sc.textFile(datapath);
        System.out.println("Ta arxika mou data "+dataRDD.take(10));
        
        JavaRDD<DataFeature> cleaned = dataRDD.map(new Function<String, DataFeature>() {
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
        }).cache();
        //System.out.println("Ta katharismena einai "+cleaned.take(10));
        JavaRDD<LabeledPoint> labeledPointJavaRDD = cleaned.map(new Function<DataFeature, LabeledPoint>() {
            public LabeledPoint call(DataFeature dataFeature) throws Exception {


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
                LabeledPoint labeledPoint = new LabeledPoint(dataFeature.getLable(), dv);
                return labeledPoint;
            }
        });
        System.out.println("i teliki morfi tou datou einai "+labeledPointJavaRDD.take(20));
        
        
        // Split the data into training and test sets (30% held out for testing)
        // transformation of labeledPointJavaRdd se pinaka me 2 splits
        // to action einai pou to kanei kati allo pera apo rdd 
        JavaRDD<LabeledPoint>[] splits = labeledPointJavaRDD.randomSplit(new double[]{0.8, 0.2});
        
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];
        /*JavaRDD<LabeledPoint> training = labeledPointJavaRDD.sample(false, 0.6, 11L);
        training.cache();
        JavaRDD<LabeledPoint> test = labeledPointJavaRDD.subtract(training);*/
        int numIterations = 10000;
        System.out.println("ta test data einai"+ testData.count());
        System.out.println("ta training data einai "+ trainingData.count());
        final SVMModel model = SVMWithSGD.train(trainingData.rdd(), numIterations);


        JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {

            public Tuple2<Double, Double> call(LabeledPoint p) {
                return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
            }
        });
        
        // previous 
         Double testMSE = predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {

            public Double call(Tuple2<Double, Double> pl) {
                Double diff = pl._1() - pl._2();
                return diff * diff;
            }
        }).reduce(new Function2<Double, Double, Double>() {

            public Double call(Double a, Double b) {
                return a + b;
            }
        }) / labeledPointJavaRDD.count(); 
        
         Double accuracy = (double)predictionAndLabel.filter(new Function<Tuple2<Double,Double>, Boolean>() {
			
			public Boolean call(Tuple2<Double, Double> v1) throws Exception {
				// TODO Auto-generated method stub
				if ((double)v1._1()== (double)v1._2()){
					return true ;
				}
				return false;
			}
		}).count()/(double)testData.count();
        
        
  
        System.out.println("i akrivia tou svm einai " + accuracy);
        sc.close();
    }
}
