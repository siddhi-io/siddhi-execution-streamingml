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
package io.siddhi.extension.execution.streamingml.bayesian.util;

import io.siddhi.extension.execution.streamingml.bayesian.model.NormalDistribution;
import org.apache.log4j.Logger;
import org.nd4j.autodiff.samediff.SDVariable;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.util.Arrays;

/**
 * implements Bayesian Linear regression model.
 * <p>
 * minimize the negative ELBO
 * ELBO = E[log(P(yIn|xIn,weights)] - D_KL[weights,prior]
 */
public class LinearRegression extends BayesianModel {
    private static final Logger logger = Logger.getLogger(LinearRegression.class.getName());
    private static final long serialVersionUID = -5112177245729410690L;

    private NormalDistribution weights;
    private SDVariable likelihoodScale;

    private SDVariable loss;

    public LinearRegression() {
        super();
    }

    public LinearRegression(LinearRegression model) {
        super(model);
//        likelihoodScale = model.likelihoodScale;
    }

    @Override
    SDVariable[] specifyModel() {

        // initiateModel placeholders
        this.xIn = sd.var("xIn", 1, numFeatures);
        this.yIn = sd.var("yIn", 1);

        // initiateModel trainable variables
        SDVariable weightLocVar, weightScaleVar, likelihoodScaleVar;
        weightLocVar = sd.var("wLocVar", numFeatures, 1);
        weightScaleVar = sd.var("wScaleVar", numFeatures, 1); // softplus ensures non-zero scale
        likelihoodScaleVar = sd.var("likScaleVar", 1, 1);
        likelihoodScale = sd.softplus(likelihoodScaleVar);

        // construct the variational distribution for weights
        weights = new NormalDistribution(weightLocVar, sd.softplus(weightScaleVar), sd);
        // construct the prior distribution for weigths
//        NormalDistribution prior = new NormalDistribution(sd.var(Nd4j.ones(numFeatures, 1).mul(priorLoc)),
//                sd.var(Nd4j.ones(numFeatures, 1).mul(priorScale)), sd);

        // computing the log-likelihood loss
        SDVariable[] logpArr = new SDVariable[numSamples];
        for (int i = 0; i < numSamples; i++) {
            SDVariable mu = xIn.mmul(weights.sample()); // linear regression
            NormalDistribution likelihood = new NormalDistribution(mu, likelihoodScale, sd);
            logpArr[i] = likelihood.logProbability(yIn);
        }
        SDVariable logpLoss = sd.neg(sd.mergeAvg(logpArr));

        loss = logpLoss;

        return new SDVariable[]{weightLocVar, weightScaleVar, likelihoodScaleVar};
//        try {
//            SDVariable klLoss = weights.klDivergence(prior);
//            loss = logpLoss.add(klLoss);
//        } catch (SiddhiAppCreationException e) {
//            loss = logpLoss;
//        }

        // setting the updaters
    }

    @Override
    double predictionFromPredictiveDensity(INDArray predictiveDistribution) {
        return predictiveDistribution.mean(1).toDoubleVector()[0];
    }

    @Override
    double confidenceFromPredictiveDensity(INDArray predictiveDistribution) {
        return 1 / (predictiveDistribution.std(1).toDoubleVector()[0]);
    }

    @Override
    protected double[][] getUpdatedWeights() {
        logger.debug(Arrays.toString(weights.getLoc().getArr().toDoubleVector()));
        return new double[][]{weights.getLoc().getArr().toDoubleVector(),
                weights.getScale().getArr().toDoubleVector()};
    }

    @Override
    public double evaluate(double[] features, Object expected) {
        return 0;
    }

    @Override
    INDArray estimatePredictiveDistribution(INDArray features, int nSamples) {
        INDArray loc, scale, scalePrediction;

        loc = this.weights.getLoc().getArr();
        scale = this.weights.getScale().getArr();
        scalePrediction = this.likelihoodScale.getArr();

        INDArray zSamples = Nd4j.randn(new long[]{numFeatures, nSamples});
        INDArray samplesLik = Nd4j.randn(new long[]{1, nSamples});

        INDArray weights = zSamples.mulColumnVector(scale).addColumnVector(loc);
        INDArray locPrediction = features.mmul(weights);
        return locPrediction.add(samplesLik.mul(scalePrediction));
    }

}
