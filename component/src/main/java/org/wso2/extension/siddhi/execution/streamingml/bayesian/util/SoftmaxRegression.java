/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.execution.streamingml.bayesian.util;

import io.siddhi.core.exception.SiddhiAppCreationException;
import org.apache.log4j.Logger;
import org.nd4j.autodiff.samediff.SDVariable;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.wso2.extension.siddhi.execution.streamingml.bayesian.model.CategoricalDistribution;
import org.wso2.extension.siddhi.execution.streamingml.bayesian.model.NormalDistribution;

import java.util.ArrayList;
import java.util.List;

import static org.nd4j.linalg.ops.transforms.Transforms.softmax;

/**
 * implements Bayesian Softmax regression model.
 * <p>
 * minimize the negative ELBO
 * ELBO = E[log(P(yIn|xIn,weights)] - D_KL[weights,prior]
 */
public class SoftmaxRegression extends BayesianModel {

    private static final Logger logger = Logger.getLogger(SoftmaxRegression.class.getName());
    private static final long serialVersionUID = 3330926145654494163L;

    private NormalDistribution weights;
    private SDVariable loss;
    private List<String> classes = new ArrayList<String>();

    // model specific configurations
    private int noOfClasses;

    // evaluation
    private PrequentialEvaluation eval;


    public SoftmaxRegression(int numberOfClasses) {
        super();
        noOfClasses = numberOfClasses;
        eval = new PrequentialEvaluation();
    }

    public SoftmaxRegression(SoftmaxRegression model) {
        super(model);
        noOfClasses = model.noOfClasses;
        eval = model.eval;
    }

    @Override
    SDVariable[] specifyModel() {

        // initiateModel placeholders
        this.xIn = sd.var("xIn", 1, numFeatures);
        this.yIn = sd.var("yIn", 1);


        // initiateModel trainable variables
        SDVariable weightLoc, weightScale;
        weightLoc = sd.var("wLoc", numFeatures, noOfClasses);
        weightScale = sd.softplus("wScale", sd.var(numFeatures, noOfClasses)); // softplus ensures non-zero scale

        // construct the variational distribution for weights
        weights = new NormalDistribution(weightLoc, weightScale, sd);
        // construct the prior distribution for weigths
//        NormalDistribution prior = new NormalDistribution(sd.var(Nd4j.ones(numFeatures, 1).mul(priorLoc)),
//                sd.var(Nd4j.ones(numFeatures, 1).mul(priorScale)), sd);

        // computing the log-likelihood loss
        SDVariable[] logpArr = new SDVariable[numSamples];
        for (int i = 0; i < numSamples; i++) {
            SDVariable logits = xIn.mmul(weights.sample()); // logits
            CategoricalDistribution likelihood = new CategoricalDistribution(logits, sd);
            logpArr[i] = likelihood.logProbability(yIn);
        }
        SDVariable logpLoss = sd.neg(sd.mergeAvg(logpArr));

        loss = logpLoss;

        return new SDVariable[]{weightLoc, weightScale};
//        try {
//            SDVariable klLoss = weights.klDivergence(prior);
//            loss = logpLoss.add(klLoss);
//        } catch (SiddhiAppCreationException e) {
//            loss = logpLoss;
//        }

    }

    @Override
    double predictionFromPredictiveDensity(INDArray predictiveDistribution) {
        return predictiveDistribution.mean(1).argMax().toDoubleVector()[0];
    }

    @Override
    double confidenceFromPredictiveDensity(INDArray predictiveDistribution) {
        return predictiveDistribution.mean(1).max().toDoubleVector()[0];
    }


    @Override
    protected double[][] getUpdatedWeights() {
        return new double[][]{weights.getLoc().getArr().reshape(-1).toDoubleVector(),
                weights.getScale().getArr().reshape(-1).toDoubleVector()};
    }

    @Override
    public double evaluate(double[] features, Object expected) {
        String target = (String) expected;
        int targetIndex = addClass(target);
        int predictedIndex = predict(features).intValue();
        return eval.evaluate(targetIndex, predictedIndex);
    }

    @Override
    INDArray estimatePredictiveDistribution(INDArray features, int nSamples) {
        INDArray loc, scale;

        loc = this.weights.getLoc().getArr().reshape(numFeatures * noOfClasses, 1);
        scale = this.weights.getScale().getArr().reshape(numFeatures * noOfClasses, 1);

        INDArray zSamples = Nd4j.randn(new long[]{numFeatures * noOfClasses, nSamples});

        INDArray weights = zSamples.mulColumnVector(scale).
                addColumnVector(loc).reshape(new int[]{numFeatures, noOfClasses * nSamples});
        INDArray predLogits = features.mmul(weights).reshape(noOfClasses, nSamples);
        return softmax(predLogits.transpose()).transpose();
    }

    public void setNoOfClasses(int val) {
        noOfClasses = val;
    }

    public double[] update(double[] features, String target) {
        int classIndex = addClass(target);
        double[] onehotTarget = new double[noOfClasses];
        onehotTarget[classIndex] = 1;
        return super.update(features, onehotTarget);
    }

    /**
     * Add a Class label.
     *
     * @param label class labels to be registered
     * @return the index of the class label if it is already registered
     */
    private int addClass(String label) {
        // Set class value
        if (classes.contains(label)) {
            return classes.indexOf(label);
        } else {
            if (classes.size() < noOfClasses) {
                classes.add(label);
                return classes.indexOf(label);
            } else {
                throw new SiddhiAppCreationException(String.format("Only %s classes are expected by the model "
                        + ". But found %s", noOfClasses, classes.size() + 1));
            }
        }
    }

    public String getClassLabel(Number index) {
        return classes.get(index.intValue());
    }

}
