package org.sparkexample.classifiers;

public class NaiveBayesModel {

    public double calculatePxgivenCUsingSimpleNaive(double[] possibilityOfXEq1,
                                                    double[] possibilityOfXEq0, double[] featureForClassification) {
        double pXgivenC = 1.0d;
        for (int i = 0; i < featureForClassification.length; i++) {
            if ((featureForClassification[i] == 1.0)) {
                pXgivenC = pXgivenC * possibilityOfXEq1[i];
            } else {
                pXgivenC = pXgivenC * possibilityOfXEq0[i];
            }
        }
        return pXgivenC;

    }

    public double calculatePxgivenCUsingBernouliNaive(double[] possibilityOfXEq1,
                                                      double[] possibilityOfXEq0, double[] featureForClassification) {
        double pXgivenC = 1.0d;
        for (int i = 0; i < featureForClassification.length; i++) {

            pXgivenC = pXgivenC * ((featureForClassification[i] * possibilityOfXEq1[i]) + ((1 - featureForClassification[i]) * (possibilityOfXEq0[i])));

        }
        return pXgivenC;

    }

    public double classifyUsingSimpleNaive(
            double[] possibilityOfXEq1givenCgood,
            double[] possibilityOfXEq0givenCgood,
            double[] possibilityOfXEq1givenCbad,
            double[] possibilityOfXEq0givenCbad,
            double possibilityOfCgood,
            double possibilityOfCbad,
            double[] featureForClassification) {
        double pXgivenCgood = calculatePxgivenCUsingSimpleNaive(possibilityOfXEq1givenCgood, possibilityOfXEq0givenCgood, featureForClassification);
        double pXgivenCbad = calculatePxgivenCUsingSimpleNaive(possibilityOfXEq1givenCbad, possibilityOfXEq0givenCbad, featureForClassification);
        if ((pXgivenCgood * possibilityOfCgood) > (pXgivenCbad * possibilityOfCbad)) {
            return (double) 1;
        }
        return (double) 0;

    }

    public double classifyUsingBernouliNaive(
            double[] possibilityOfXEq1givenCgood,
            double[] possibilityOfXEq0givenCgood,
            double[] possibilityOfXEq1givenCbad,
            double[] possibilityOfXEq0givenCbad,
            double possibilityOfCgood,
            double possibilityOfCbad,
            double[] featureForClassification) {
        double pXgivenCgood = calculatePxgivenCUsingBernouliNaive(possibilityOfXEq1givenCgood, possibilityOfXEq0givenCgood, featureForClassification);
        double pXgivenCbad = calculatePxgivenCUsingBernouliNaive(possibilityOfXEq1givenCbad, possibilityOfXEq0givenCbad, featureForClassification);
        if ((pXgivenCgood * possibilityOfCgood) > (pXgivenCbad * possibilityOfCbad)) {
            return (double) 1;
        }
        return (double) 0;

    }

}
