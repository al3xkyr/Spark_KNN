package org.sparkexample.classifiers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.sparkexample.pojo.PojoRow;

public class AccuracyManager {

    InitialParameters baseModel;
    AmmendManager ammendedClassifier;
    List<PojoRow> list;

    public double getAccuracy() {
        double accuracy = 0;
        double countOfCorrect = 0;
        for (PojoRow p : list) {
            double toDouble = p.label;
            double prediction = new NaiveBayesModel().classifyUsingBernouliNaive(
                    baseModel.getPossibilityOfXEq1givenCgood(), baseModel.getPossibilityOfXEq0givenCgood(),
                    baseModel.getPossibilityOfXEq1givenCbad(), baseModel.getPossibilityOfXEq0givenCbad(),
                    baseModel.getPosCgood(),
                    baseModel.getPosCbad(), p.features.toArray());
            if (prediction == toDouble) {
                countOfCorrect++;
            }
        }
        accuracy = countOfCorrect / (double) list.size();

        return accuracy;
    }

//    public double getAccuracyOnaSetWithAmmendedManager() {
//        double accuracy = 0;
//        double countOfCorrect = 0;
//
//        for (PojoRow p : list) {
//            double toDouble = p.label;
//
//            double prediction = new NaiveBayesModel().classifyUsingBernouliNaive(
//                    ammendedClassifier.getPossibilityOfXEq1givenCgood(),
//                    ammendedClassifier.getPossibilityOfXEq0givenCgood(),
//                    ammendedClassifier.getPossibilityOfXEq1givenCbad(),
//                    ammendedClassifier.getPossibilityOfXEq0givenCbad(),
//                    ammendedClassifier.getPosCgood(),
//                    ammendedClassifier.getPosCbad(),
//                    p.features.toArray());
//            if (prediction == toDouble) {
//                countOfCorrect++;
//            }
//        }
//        accuracy = countOfCorrect / list.size();
//        return accuracy;
//
//    }

    public List<PojoRow> getCrossValidatedTweets() {
        List<PojoRow> validatedList = new ArrayList<PojoRow>();
        for (PojoRow pojo : list) {

            if (new NaiveBayesModel().classifyUsingSimpleNaive(
                    baseModel.getPossibilityOfXEq1givenCgood(), baseModel.getPossibilityOfXEq0givenCgood(),
                    baseModel.getPossibilityOfXEq1givenCbad(), baseModel.getPossibilityOfXEq0givenCbad(),
                    baseModel.getPosCgood(),
                    baseModel.getPosCbad(), pojo.features.toArray()) == pojo.label) {
                validatedList.add(pojo);
            }

        }
        return validatedList;

    }


    public List<PojoRow> getList() {
        return list;
    }

    public void setList(List<PojoRow> list) {
        this.list = list;
    }

    public InitialParameters getBaseModel() {
        return baseModel;
    }

    public void setBaseModel(InitialParameters baseModel) {
        this.baseModel = baseModel;
    }

    public AmmendManager getAmmendedClassifier() {
        return ammendedClassifier;
    }

    public void setAmmendedClassifier(AmmendManager ammendedClassifier) {
        this.ammendedClassifier = ammendedClassifier;
    }
}
