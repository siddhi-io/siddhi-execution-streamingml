/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.execution.streamingml.classification.perceptron.util;

import io.siddhi.extension.execution.streamingml.util.MathUtil;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Represents a linear Perceptron Model
 */
public class PerceptronModel implements Serializable {
    private static final long serialVersionUID = -5179648194841293764L;
    private double[] weights;
    private double bias = 0.0;
    private double threshold = 0.5;
    private double learningRate = 0.1;

    public PerceptronModel() {
    }

    public PerceptronModel(PerceptronModel model) {
        this.weights = model.weights;
        this.bias = model.bias;
        this.threshold = model.threshold;
        this.learningRate = model.learningRate;
    }

    /**
     * Mehod to update weight of perceptron model
     * @param label target class/label value
     * @param features array of feature attributes to train
     * @return array of updated weights
     */
    public double[] update(Boolean label, double[] features) {
        boolean predictedLabel = this.classify(this.getPredictionProbability(features));

        if (!label.equals(predictedLabel)) {
            double error = Boolean.TRUE.equals(label) ? 1.0 : -1.0;

            // Get correction
            double correction;
            for (int i = 0; i < features.length; i++) {
                correction = features[i] * error * this.learningRate;
                this.weights[i] = this.weights[i] + correction;
            }
        }
        return Arrays.copyOf(weights, weights.length);
    }

    private double getPredictionProbability(double[] features) {
        if (this.weights == null) {
            this.initWeights(features.length);
        }
        return MathUtil.dot(features, weights) + this.bias;
    }

    private boolean classify(double evaluation) {
        return evaluation > this.threshold ? true : false;
    }

    /**
     * Classify the feature set into class
     * @param features set of feature attributed
     * @return prediction and its probability
     */
    public Object[] classify(double[] features) {
        double evaluation = getPredictionProbability(features);
        boolean prediction = classify(evaluation);
        return new Object[]{prediction, evaluation};
    }

    /**
     * Initialize model's weights
     * @param size
     */
    public void initWeights(int size) {
        this.weights = new double[size];
    }

    /**
     * Get model's feature set size
     * @return Size of the feature vector
     */
    public int getFeatureSize() {
        if (weights == null) {
            return -1;
        }
        return weights.length;
    }

    /**
     * Set a bias for the model
     * @param bias bias to shift the decision boundary
     */
    public void setBias(double bias) {
        this.bias = bias;
    }

    /**
     * Set a threshold to the model
     * @param threshold boundary for the prediction
     */
    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    /**
     * Set a learning rate to the model
     * @param learningRate To avoid the local optima
     */
    public void setLearningRate(double learningRate) {
        this.learningRate = learningRate;
    }
}
