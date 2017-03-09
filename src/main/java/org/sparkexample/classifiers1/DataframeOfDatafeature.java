package org.sparkexample.classifiers1;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.sparkexample.pojo1.DataFeature;

import scala.Function1;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class DataframeOfDatafeature {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sQLContext = new SQLContext(sc);

		// Load and parse the data file.
		String datapathPanos = "C:/Users/pataris/Desktop/птувиайг SENTIMENT ANALYSIS/DATA/60000tweets.csv";

		Dataset<Row> dataPanos = sQLContext.read().csv(datapathPanos);
		List<Row> val1 = dataPanos.collectAsList();
		System.out.println(val1.get(0));
		
		long val = dataPanos.count();
		System.out.println(val);
		// transformation on dataset to be cleaned and create the datafeature
		Encoder<Double> doubleEncoder = Encoders.DOUBLE();
		
		
		
	}
}
