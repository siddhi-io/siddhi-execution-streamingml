/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.execution.ml.samoa.utils.classification;

import org.wso2.extension.siddhi.execution.ml.samoa.utils.StreamingProcess;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;

import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamingClassification extends StreamingProcess {

    private int numberOfAttributes;

    private Queue<Vector> samoaClassifiers; // Output prediction data

    public StreamingClassification(int maxEvents, int interval, int classes, int parameterCount,
        int nominals, String nominalAttributesValues, int parallelism, int bagging) {

        this.numberOfAttributes = parameterCount;
        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaClassifiers = new ConcurrentLinkedQueue<Vector>();

        try {
            this.processTaskBuilder = new StreamingClassificationTaskBuilder(maxEvents, interval,
                    classes, this.numberOfAttributes, nominals, nominalAttributesValues,
                    this.cepEvents, this.samoaClassifiers,
                    parallelism, bagging);
            // TODO: 12/23/16 need a specific exception
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Fail to initialize the Streaming " +
                    "Classification : ", e);
        }
    }

    public Object[] getOutput() {
        Object[] output;
        if (!samoaClassifiers.isEmpty()) {
            Vector prediction = samoaClassifiers.poll();
            output = prediction.toArray();
        } else {
            output = null;
        }
        return output;
    }
}
