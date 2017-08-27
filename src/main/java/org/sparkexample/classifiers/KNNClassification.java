package org.sparkexample.classifiers;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.classification.KNNClassificationModel;
import org.apache.spark.ml.classification.KNNClassifier;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sparkexample.pojo.PojoRow;

import scala.Tuple2;


public class KNNClassification implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	SparkSession sqlSpark ;
	KNNClassificationModel model;
	
	public KNNClassification (JavaRDD<PojoRow> trainingData , SparkContext sc1){
		this.sqlSpark = new SparkSession(sc1);
		Dataset<Row> labeledPointDataset = sqlSpark.createDataFrame(trainingData, PojoRow.class);
		// Knn classification
		KNNClassifier val = new KNNClassifier().setTopTreeSize(2).setK(5);
		val.train(labeledPointDataset);

		this.model = val.fit(labeledPointDataset);
		// predict on label point for Dataset
		// val.transform(labeledPointDataset, model.topTree(),
		// model.subTrees());
		// Creation of Dataset of testData for KNN
		
	}
	public List<PojoRow> getPredictedRDD (JavaRDD<PojoRow> testDataForValidation){
		Dataset<Row> labeledPointDatasetforTest = sqlSpark.createDataFrame(testDataForValidation, PojoRow.class);

		Dataset<Row> sss = model.transform(labeledPointDatasetforTest);
		sss.show();
		Dataset<Row> j32 = sss.select("features", "label", "prediction");
		j32.show();
		JavaPairRDD<Double, Double> ewo = j32.select("prediction", "label").toJavaRDD()
				.mapToPair(new PairFunction<Row, Double, Double>() {

					public Tuple2<Double, Double> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Double, Double>(t.getDouble(0), t.getDouble(1));
					}
				});
		double accuracyOfKNNOntestDataForValidation = ewo.filter(new Function<Tuple2<Double, Double>, Boolean>() {

			public Boolean call(Tuple2<Double, Double> v1) throws Exception {
				// TODO Auto-generated method stub
				if (v1._1.doubleValue() == v1._2.doubleValue()) {
					return true;
				}
				return false;
			}
		}).count() / (double) (testDataForValidation.count());
		System.out.println("the accuaracy of knn is " + accuracyOfKNNOntestDataForValidation);

		// creating new rdd with label the prediction on test data
		JavaRDD<PojoRow> trainDataForNaiveWithPredictionsOfKNN = j32.select("prediction", "features").toJavaRDD()
				.map(new Function<Row, PojoRow>() {

					public PojoRow call(Row t) throws Exception {
						// TODO Auto-generated method stub
						return new PojoRow((Double) t.get(0), (Vector) t.get(1));
					}
				});
		List<PojoRow> traindataCollectedwithLabelFromKNNFromTestData = trainDataForNaiveWithPredictionsOfKNN
				.collect();
		// List<PojoRow> traindataCollectedwithLabelFromKNNFromTestData =
		// testData.collect();
		// end of knn
		return traindataCollectedwithLabelFromKNNFromTestData;
	}

}
