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

package org.wso2.extension.siddhi.execution.ml.samoa.utils;

import java.util.Queue;

/**
 * Streaming Process
 */
public abstract class StreamingProcess implements Runnable {

    public Queue<double[]> cepEvents;
    protected TaskBuilder processTaskBuilder;

    public abstract Object[] getOutput();

    public void run() {
        processTaskBuilder.initTask();
        processTaskBuilder.submit();
    }

    public void addEvents(double[] eventData) {
        cepEvents.add(eventData);
    }
}
