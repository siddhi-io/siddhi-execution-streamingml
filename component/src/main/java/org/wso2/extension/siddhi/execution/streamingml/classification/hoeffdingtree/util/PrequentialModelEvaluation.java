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
package org.wso2.extension.siddhi.execution.streamingml.classification.hoeffdingtree.util;

import com.yahoo.labs.samoa.instances.Instance;
import org.wso2.extension.siddhi.execution.streamingml.util.CoreUtils;

/**
 * Prequential or interleaved-test-then-train evolution.
 * Each instance is first to test the model, and then to train the model.
 * The Prequential Evaluation task evaluates the performance of online classifiers doing this.
 * Measures the accuracy of the classifier model since the start of the evaluation
 */
public class PrequentialModelEvaluation {
    private int numClasses = -1;

    private double weightObserved;
    private double weightCorrect;

    public void reset(int numClasses) {
        this.numClasses = numClasses;
        this.weightObserved = 0.0D;
        this.weightCorrect = 0.0D;
    }

    /**
     * @param inst       MOAInstance representing the cepEvent
     * @param classVotes Prediction votes for each class label
     */
    void addResult(Instance inst, double[] classVotes) {
        if (this.numClasses == -1) {
            this.reset(inst.numClasses());
        }
        double weight = inst.weight();
        int trueClass = (int) inst.classValue();

        if (weight > 0.0D) {
            if (this.weightObserved == 0.0D) {
                this.reset(inst.numClasses());
            }
            this.weightObserved += weight;
            int predictedClass = CoreUtils.argMaxIndex(classVotes);
            if (predictedClass == trueClass) {
                this.weightCorrect += weight;
            }
        }
    }

    double getFractionCorrectlyClassified() {
        return this.weightObserved > 0.0D ? this.weightCorrect / this.weightObserved : 0.0D;
    }


}
