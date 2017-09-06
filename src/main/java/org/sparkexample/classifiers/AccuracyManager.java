package org.sparkexample.classifiers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.sparkexample.pojo.PojoRow;

public class AccuracyManager {
    public double getAccuracyOnaSetWithInitalParameter(List<PojoRow> list, InitialParameters baseModel) {
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

    public double getAccuracyOnaSetWithAmmendedManager(List<PojoRow> list, AmmendManager ammendedClassifier) {
        double accuracy = 0;
        double countOfCorrect = 0;

        for (PojoRow p : list) {
            double toDouble = p.label;
            System.out.println("Ammended 1 given good " + Arrays.toString(ammendedClassifier.getPossibilityOfXEq1givenCgood()));
//			System.out.println("Ammended 0 given good "+ Arrays.toString(ammendedClassifier.getPossibilityOfXEq0givenCgood()));
//			System.out.println("Ammended 1 given bad "+ Arrays.toString(ammendedClassifier.getPossibilityOfXEq1givenCbad()));
//			System.out.println("Ammended 0 given bad "+ Arrays.toString(ammendedClassifier.getPossibilityOfXEq0givenCbad()));
//			System.out.println("Ammended pos number good"+ ammendedClassifier.getPosCgood());
//			System.out.println("Ammended pos number bad "+ ammendedClassifier.getPosCbad());


            double prediction = new NaiveBayesModel().classifyUsingBernouliNaive(
                    ammendedClassifier.getPossibilityOfXEq1givenCgood(),
                    ammendedClassifier.getPossibilityOfXEq0givenCgood(),
                    ammendedClassifier.getPossibilityOfXEq1givenCbad(),
                    ammendedClassifier.getPossibilityOfXEq0givenCbad(),
                    ammendedClassifier.getPosCgood(),
                    ammendedClassifier.getPosCbad(),
                    p.features.toArray());
            if (prediction == toDouble) {
                countOfCorrect++;
            }
        }
        accuracy = countOfCorrect / list.size();
        return accuracy;

    }

    public List<PojoRow> getCrossValidatedTweets(List<PojoRow> list, InitialParameters baseModel) {
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
}
