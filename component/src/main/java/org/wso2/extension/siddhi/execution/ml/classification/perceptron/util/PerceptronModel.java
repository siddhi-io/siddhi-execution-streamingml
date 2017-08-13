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
package org.wso2.extension.siddhi.execution.ml.classification.perceptron.util;

import org.wso2.extension.siddhi.execution.ml.util.MathUtil;

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

    public double[] update(Boolean label, double[] features) {
        boolean predictedLabel = this.classifyToClass(features);

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

    public double classify(double[] features) {
        if (this.weights == null) {
            this.initWeights(features.length);
        }
        double evaluation = MathUtil.dot(features, weights) + this.bias;
        return evaluation;
    }

    public boolean classifyToClass(double[] features) {
        double evaluation = classify(features);
        boolean prediction = evaluation > this.threshold ? true : false;
        return prediction;
    }

    public void initWeights(int size) {
        this.weights = new double[size];
    }

    public int getFeatureSize() {
        if (weights == null) {
            return -1;
        }
        return weights.length;
    }

    public void setBias(double bias) {
        this.bias = bias;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public void setLearningRate(double learningRate) {
        this.learningRate = learningRate;
    }
}
