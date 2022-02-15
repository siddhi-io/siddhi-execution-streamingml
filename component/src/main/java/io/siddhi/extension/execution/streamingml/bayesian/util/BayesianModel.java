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

import io.siddhi.core.exception.SiddhiAppCreationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nd4j.autodiff.samediff.SDVariable;
import org.nd4j.autodiff.samediff.SameDiff;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.GradientUpdater;
import org.nd4j.linalg.learning.config.AdaGrad;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.learning.config.IUpdater;
import org.nd4j.linalg.learning.config.Nadam;
import org.nd4j.linalg.learning.config.Sgd;

import java.io.Serializable;

/**
 * model interface.
 */
public abstract class BayesianModel implements Serializable {

    private static final Logger logger = LogManager.getLogger(BayesianModel.class.getName());
    private static final long serialVersionUID = -3217237991548906395L;

    SameDiff sd;
    SDVariable xIn, yIn;

    // configurable params
    int numFeatures;
    int numSamples;
    private boolean addBias;
    private int predictionSamples;
    // config params to construct the optimizer
    private OptimizerType optimizerType;
    private double learningRate;

    // vars and gradient updaters
    private SDVariable[] vars;
    private GradientUpdater[] updaters;

    // params used for restoring the model
    private IUpdater optimizer;
    private INDArray[] weightStates;
    private INDArray[] viewArrays;


    /**
     * builds the model.
     * <p>
     * sets the default configurations
     */
    BayesianModel() {
        this.numFeatures = -1;
        this.numSamples = 1;
        this.addBias = false;

        this.optimizerType = OptimizerType.ADAM;
        this.learningRate = 0.05;

        this.predictionSamples = 1000;
    }

    BayesianModel(BayesianModel model) {
        this.numFeatures = model.numFeatures;
        this.numSamples = model.numSamples;
        this.addBias = model.addBias;

        this.predictionSamples = model.predictionSamples;

        this.optimizer = model.optimizer;
        this.weightStates = model.weightStates;

        this.optimizerType = model.optimizerType;
        this.learningRate = model.learningRate;
        this.viewArrays = model.viewArrays;
    }

    /**
     * construct the model.
     */
    public void initiateModel() {
        // initiateModel class variables
        sd = SameDiff.create();

        // additional feature dimension for bias
        if (addBias) {
            numFeatures += 1;
        }

        // collect variables
        vars = specifyModel();
        int numOfVars = vars.length;

        // whether a restoration or initiation
        if (weightStates == null) { // initiation

            // initiate the optimizer
            optimizer = createUpdater();
            // initialize the state arrays.
            weightStates = new INDArray[numOfVars];
            viewArrays = new INDArray[numOfVars];

            for (int i = 0; i < numOfVars; i++) { // store the pointers to the states
                weightStates[i] = vars[i].getArr();

                long varSize = 1;
                for (long shape : vars[i].getShape()) {
                    varSize *= shape;
                }

                viewArrays[i] = Nd4j.create(1, optimizer.stateSize(varSize));
            }


        } else { // restoration
            if (vars.length != weightStates.length) {
                throw new SiddhiAppCreationException(String.format("Failed to restore model due to " +
                                "unmatched number of variables. Expected %d variables. Given %d",
                        vars.length, weightStates.length));
            }
            if (viewArrays == null || viewArrays.length != vars.length) {
                throw new SiddhiAppCreationException("Failed recovering the state of the model. " +
                        "Invalid state for the gradient updaters");
            }
//            if (optimizer == null) {
//                throw new SiddhiAppCreationException("Failed recovering the state of the model. " +
//                        "Invalid optimizer");
//            }

            for (int i = 0; i < vars.length; i++) {
                try {
                    vars[0].setArray(weightStates[i]); // restore the weights
                } catch (Exception ex) {
                    throw new SiddhiAppCreationException("Failed recovering the state of the gradient updaters. " +
                            "Invalid state for the variables");
                }
            }
        }

        // currently the GradientUpdater is not serializable.
        // and different gradient updaters can have different states stored.
        // therefore stores the view array of the each gradient updater
        // initialize the gradient updaters
        updaters = new GradientUpdater[vars.length];
        for (int i = 0; i < numOfVars; i++) {
            if (OptimizerType.SGD.equals(optimizerType)) {
                updaters[i] = optimizer.instantiate(null, true);
            } else {
                updaters[i] = optimizer.instantiate(viewArrays[i], true);
            }
        }
        logger.debug("Successfully initiated gradient optimizer : " + optimizer.getClass().getSimpleName());
    }

    /**
     * updates the variables list stored @vars.
     */
    private void updateVariables() {
        for (int i = 0; i < vars.length; i++) {
            SDVariable var = vars[i];
            INDArray gradients = var.getGradient().getArr();
            long[] gradientShape = gradients.shape();

            INDArray gradientArr = Nd4j.toFlattened(gradients);
            if (Double.isNaN(gradients.mean().toDoubleVector()[0])) {
                logger.warn(String.format("invalid gradients. skipping variable update of %s", var.getVarName()));
                return;
            }

            // apply updater to the gradients
            // we set the iteration and epochs to 1.
            // incrementing a integer in online settings may results overflow
            updaters[i].applyUpdater(gradientArr, 1, 0);

            // gradient descent step
            var.setArray(var.getArr().sub(gradientArr.reshape(gradientShape)));
        }

    }

    /**
     * train the model.
     *
     * @param features feature vector
     * @param target   target/label
     *                 for regression target should be a real vector
     *                 for binary classification target should be a vector with labels (0 or 1)
     *                 multiclass classification expects one-hot embedded matrix or a vector with label indexes
     */
    public double[] update(double[] features, double[] target) {

        INDArray featureArr = Nd4j.create(features);
        INDArray targetArr = Nd4j.create(target);

        if (addBias) {
            featureArr = Nd4j.append(featureArr, 1, 1, 1);
        }
        xIn.setArray(featureArr);
        yIn.setArray(targetArr);

        INDArray loss = sd.execAndEndResult();
        sd.execBackwards();

        logger.info(this.getClass().getName() + " model loss : " + loss.toString());

        updateVariables();

        return loss.toDoubleVector();
    }

    /**
     * predict the target according to given features.
     * predictive distribution is approximated using nSamples from the actual distribution
     * <p>
     * uses 1000 (default) samples to estimate the predictive distribution
     *
     * @param features feature vector
     * @return only the mean of the predictions
     */
    public Double predict(double[] features) {
        INDArray featureArr = Nd4j.create(features);
        if (addBias) {
            featureArr = Nd4j.append(featureArr, 1, 1, 1);
        }
        INDArray predictiveDistribution = estimatePredictiveDistribution(featureArr, predictionSamples);
        return predictionFromPredictiveDensity(predictiveDistribution);
    }

    /**
     * predict the target according to given features.
     * predictive distribution is approximated using nSamples from the actual distribution
     * <p>
     * uses 1000 (default) samples to estimate the predictive distribution
     *
     * @param features feature vector
     * @return both mean of the predictions and the std
     */
    public Double[] predictWithStd(double[] features) {
        INDArray featureArr = Nd4j.create(features);
        logger.info(featureArr.toString());
        if (addBias) {
            featureArr = Nd4j.append(featureArr, 1, 1, 1);
        }
        INDArray predictiveDistribution = estimatePredictiveDistribution(featureArr, predictionSamples);
        return new Double[]{predictionFromPredictiveDensity(predictiveDistribution),
                confidenceFromPredictiveDensity(predictiveDistribution)};
    }

    /**
     * implements the model specific gradient updates.
     *
     * @return var means and stds
     */
    protected abstract double[][] getUpdatedWeights();


    public abstract double evaluate(double[] features, Object expected);

    /**
     * implements the model specific methods to estimate the predictive distributions.
     * predictive distribution is approximated using nSamples from the actual distribution
     *
     * @param features feature vector
     * @param nSamples number of samples used for approximation
     * @return predictive densities
     */
    abstract INDArray estimatePredictiveDistribution(INDArray features, int nSamples);

    /**
     * implements the specific model structure.
     *
     * @return var list to register for gradient updates
     */
    abstract SDVariable[] specifyModel();

    /**
     * implements any post processing required for the mean of the predictive distribution.
     * <p>
     * ex : softmax regression require labels instead of mean of the softmax values
     *
     * @param predictiveDistribution predictive density
     * @return formatted prediction
     */
    abstract double predictionFromPredictiveDensity(INDArray predictiveDistribution);


    /**
     * implements any post processing required for the confidence of the prediction.
     * <p>*
     *
     * @param predictiveDistribution predictive mean of the predictive density
     * @return confidence
     */
    abstract double confidenceFromPredictiveDensity(INDArray predictiveDistribution);

    private IUpdater createUpdater() {
        switch (optimizerType) {
            case ADAM:
                return new Adam(learningRate);
            case SGD:
                return new Sgd(learningRate);
            case ADAGRAD:
                return new AdaGrad(learningRate);
            case NADAM:
                return new Nadam(learningRate);
            default:
                return new Adam(learningRate);

        }
    }

    public void setAddBias(boolean val) {
        addBias = val;
    }

    public int getNumFeatures() {
        return numFeatures;
    }

    public void setNumFeatures(int val) {
        numFeatures = val;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public void setNumSamples(int val) {
        numSamples = val;
    }

    public OptimizerType getOptimizerType() {
        return optimizerType;
    }

    public void setOptimizerType(OptimizerType val) {
        optimizerType = val;
    }

    public double getLearningRate() {
        return learningRate;
    }

    public void setLearningRate(double val) {
        learningRate = val;
    }

    public void setPredictionSamples(int val) {
        predictionSamples = val;
    }


    /**
     * optimizer types that can be used with the bayesian models.
     */
    public enum OptimizerType {
        ADAM, ADAGRAD, SGD, NADAM
    }
}

