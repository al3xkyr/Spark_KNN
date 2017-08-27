package org.sparkexample.classifiers1;

import java.io.Serializable;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.sparkexample.pojo1.DataFeature;
import org.sparkexample.pojo1.PojoRow;

public class DataExtraction implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static SparkContext sc1;

	private String datapath;

	public DataExtraction(SparkContext sc1 , String datapath) {
		this.sc1 = sc1;

		this.datapath = datapath;

	}

	public JavaRDD<DataFeature> getDataCleaned() {
		JavaRDD<String> dataAsStrings = this.sc1.textFile(this.datapath, 5).toJavaRDD();
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

		return cleaned;
	}

	// transformation on dataset to be cleaned and create the datafeature
	// which has swn score and the attributes that i want to store
	// pernei to rdd apo panw kai to kanei apo RDD string se RDD apo
	// dataFeature
	public JavaRDD<PojoRow> getDatalabeledPoint() {

		JavaRDD<PojoRow> labeledPointJavaRDD = getDataCleaned().map(new Function<DataFeature, PojoRow>() {

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
				if (isMetaphorBool) {
					isMetaphor = 1;
				} else {
					isMetaphor = 0;
				}
				// ,neg,ohso,capital
				double love;
				boolean loveBool = dataFeature.getData().getBoolean("__LOVE__");
				if (loveBool) {
					love = 1;
				} else {
					love = 0;
				}
				double laugh; 
				boolean laughbool = dataFeature.getData().getBoolean("__LAUGH__");
				if(laughbool ){
					laugh = 1 ;
				} else {
					laugh = 0 ; 
				}
				// breaking the double value for total swn score based on the
				// observations about the polarity of the tweet by means
				double swn_positive = 0;
				double swn_negative = 0;
				double swn_somewhatPositive = 0;
				double swn_neutural = 0;
				double swn_somewhatNegative = 0;
				double swnScore = dataFeature.getData().getDouble("__swn_score__");
				if (swnScore > 1.2) {
					swn_positive = 1;
				} else if (swnScore <= 1.2 && swnScore > 0.95) {
					swn_somewhatPositive = 1;
				} else if (swnScore <= 0.95 && swnScore > 0.5) {
					swn_neutural = 1;
				} else if (swnScore <= 0.5 && swnScore > 0.2) {
					swn_somewhatNegative = 1;
				} else if (swnScore < 0.2) {
					swn_negative = 1;
				}
				// epilegontai features pou den einai eutheos analoga me ton
				// prosdiorismo tis klasis tou tweet
				// diladi ta positive smileys kai ta negatives einai amesa
				// sindedemena me tin akrivia tou classifier
				// double[] arrayofVector = { swn_positive, swn_negative,
				// swn_somewhatPositive, swn_neutural,
				// swn_somewhatNegative, neg, ohso, capital, isMetaphor, love };
				double[] arrayofVector = {  laugh, neg, ohso, capital, isMetaphor, love};
				Vector dv = Vectors.dense(arrayofVector);
				PojoRow rowPojoforProcess = new PojoRow(dataFeature.getLable(), dv);
				return rowPojoforProcess;
			}
		});
		return labeledPointJavaRDD;
	}
}
