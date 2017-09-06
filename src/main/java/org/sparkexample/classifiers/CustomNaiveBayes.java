package org.sparkexample.classifiers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.sparkexample.pojo.PojoRow;

import scala.Tuple2;

public class CustomNaiveBayes {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("org.sparkexample.WordCount2").setMaster("local[*]");
        /*
		 * @SuppressWarnings("resource") JavaSparkContext sc = new
		 * JavaSparkContext(sparkConf);
		 */
        SparkContext sc1 = new SparkContext(sparkConf);
        @SuppressWarnings("resource")
        SparkSession sparkSession = new SparkSession(sc1);
        // Load and parse the data file.
        // Training data 40k tweets
        String trainingDatapath = "data/60000tweetsTrain.csv";
        DataExtraction dataTrain = new DataExtraction(sc1, trainingDatapath);
        // Test and ammending data 20K tweets
        String testDatapath = "data/60000tweetsTest.csv";
        DataExtraction dataTest = new DataExtraction(sc1, testDatapath);

        // Split the data into training and test sets (30% held out for testing)

        JavaRDD<PojoRow> trainingData = dataTrain.getDatalabeledPoint();
        JavaRDD<PojoRow>[] splits = dataTest.getDatalabeledPoint().randomSplit(new double[]{0.5, 0.3, 0.2,});
        JavaRDD<PojoRow> initTestData = splits[0];

        JavaRDD<PojoRow> ammendedDataForOptimalAccuracy = splits[1];
        JavaRDD<PojoRow> ammendedDataForKNNAccuracy = splits[2];


        // getting the test data on driver
        List<PojoRow> initTestDataCollected = initTestData.collect();
        // creating the initial model
        AccuracyManager accuracyManager = new AccuracyManager();
        InitialParameters baseModel = new InitialParameters(trainingData);
        System.out.println(baseModel.toString());

        double accuracyOfInitModel = accuracyManager.getAccuracyOnaSetWithInitalParameter(initTestDataCollected, baseModel);

        // training the knn classifier
        KNNClassification knnClassifier = new KNNClassification(trainingData, sc1);

        // geting corrected data for ammending
        List<PojoRow> rightDataforAmmending = ammendedDataForOptimalAccuracy.collect();
        long countOfTrainingData = trainingData.count();
        //sparkSession.close();
        //System.out.println("pernaei apo close ");
        //storing variables in order to isolate them
        double[] possibilitiesX1C1 = baseModel.getPossibilityOfXEq1givenCgood();
        double[] possibilitiesX1C0 = baseModel.getPossibilityOfXEq1givenCbad();
        double[] possibilitiesX0C1 = baseModel.getPossibilityOfXEq0givenCgood();
        double[] possibilitiesX0C0 = baseModel.getPossibilityOfXEq0givenCbad();
        double posGood = baseModel.getPosCgood();
        double posBad = baseModel.getPosCbad();
        double goodNumber = baseModel.getGoodTweetNumber();
        double badNumber = baseModel.getGoodTweetNumber();

        AmmendManager ammendWithRightdata = new AmmendManager(possibilitiesX1C1
                , possibilitiesX1C0,
                possibilitiesX0C1,
                possibilitiesX0C0,
                posGood,
                posBad, goodNumber, badNumber,
                rightDataforAmmending,
                countOfTrainingData);
        ammendWithRightdata.ammend();
        System.out.println(ammendWithRightdata.toString());

        AccuracyManager accuracyManager1 = new AccuracyManager();
        double acccuracyOfAmmendedGoodData = accuracyManager1.getAccuracyOnaSetWithAmmendedManager(initTestDataCollected, ammendWithRightdata);
        System.out.println(ammendWithRightdata.toString());


        List<PojoRow> knnDataForAmmending = knnClassifier.getPredictedRDD(ammendedDataForKNNAccuracy);
        AmmendManager ammendWithKNNdata = new AmmendManager(possibilitiesX1C1
                , possibilitiesX1C0,
                possibilitiesX0C1,
                possibilitiesX0C0,
                posGood,
                posBad, goodNumber, badNumber,
                knnDataForAmmending,
                countOfTrainingData);

        ammendWithKNNdata.ammend();
        AccuracyManager accuracyManager2 = new AccuracyManager();

        List<PojoRow> crossValidatedData = accuracyManager2.getCrossValidatedTweets(knnDataForAmmending, baseModel);
        AmmendManager ammendWithCrossValidatedData = new AmmendManager(possibilitiesX1C1
                , possibilitiesX1C0,
                possibilitiesX0C1,
                possibilitiesX0C0,
                posGood,
                posBad, goodNumber, badNumber,
                crossValidatedData,
                countOfTrainingData);
        ammendWithCrossValidatedData.ammend();
        // --------------end of sums ---------------and probabilities
        // calculating accuracy of Naive old model in testDataForValidation


        double accuracyOfAmmendedWithKNNPredictions = accuracyManager2.getAccuracyOnaSetWithAmmendedManager(initTestDataCollected, ammendWithKNNdata);
        double accuracyOfAmmendedWithCrossValidatedData = accuracyManager2.getAccuracyOnaSetWithAmmendedManager(initTestDataCollected, ammendWithCrossValidatedData);
        ///==================================Ideal scenario of ammending good data======================================================


        /// ==================================================Scenario of KNN data =================================================
        System.out.println("Data = " + baseModel.toString());
        System.out.println("Data = " + ammendWithRightdata.toString());
        System.out.println("The accuracy of Old naive model with dataForValidation is "
                + accuracyOfInitModel);

        System.out.println("The optimal accuracy after the ammend of ammendedDataForOptimalAccuracy " + acccuracyOfAmmendedGoodData);


        System.out.println("The accuracy after the ammend from KNN classifier " + accuracyOfAmmendedWithKNNPredictions);

        System.out.println("The accuracy after the data validated is " + accuracyOfAmmendedWithCrossValidatedData);

    }


    public static boolean doesModelNeedRetrain(JavaRDD<PojoRow> trainingData, JavaRDD<PojoRow> testData) {
        double entropyOfTestDataSet = getEntropyOfADataSet(testData);
        double entropyOfTrainDataset = getEntropyOfADataSet(trainingData);


        // based on the assumption of the training dataset is chosed to be
        // balanced about positive and negative tweets
        // which means the entropy of it is close to 1
		/*if ((entropyOfTrainDataset - entropyOfTestDataSet) > 0.1) {
			return true;
		}
		return false;*/
        // dummy logic in order to continue with ammending
        if ((entropyOfTrainDataset != entropyOfTestDataSet)) {
            return true;
        }
        return false;
    }

    public static double getEntropyOfADataSet(JavaRDD<PojoRow> trainingData) {
        // entropy of whole data set
        // change from labeledPointJavaRDD to testdata
        JavaRDD<PojoRow> rddOfPositiveTweets = trainingData.filter(new Function<PojoRow, Boolean>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            public Boolean call(PojoRow v1) throws Exception {
                if (v1.getLabel() == 1.0) {
                    return true;
                }
                return false;
            }
        });
        long numOfNegativeTweets = trainingData.count() - rddOfPositiveTweets.count();
        Double possibilityOfPositive = Double.longBitsToDouble(rddOfPositiveTweets.count())
                / Double.longBitsToDouble(trainingData.count());
        Double possibilityOfNegative = Double.longBitsToDouble(numOfNegativeTweets)
                / Double.longBitsToDouble(trainingData.count());
        Double entropyOfDataset = -((possibilityOfPositive) * (Math.log(possibilityOfPositive) / Math.log(2))
                + ((possibilityOfNegative) * (Math.log(possibilityOfNegative) / Math.log(2))));
        return entropyOfDataset;

    }

}
