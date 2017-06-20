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

package org.wso2.extension.siddhi.execution.ml.samoa.utils.clustering;

import org.apache.samoa.moa.cluster.Cluster;
import org.apache.samoa.moa.cluster.Clustering;
import org.wso2.extension.siddhi.execution.ml.samoa.utils.StreamingProcess;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Streaming Clustering
 */
public class StreamingClustering extends StreamingProcess {
    private int numberOfAttributes;
    private int numberOfClusters;
    public int maxEvents;
    private int parallelism;
    private int sampleFrequency;
    private int interval;

    private Queue<Clustering> samoaClusters;

    public StreamingClustering(int maxEvent, int paramCount, int numberOfClusters, int parallel,
                               int samplefrequency, int interval) {
        this.maxEvents = maxEvent;
        this.numberOfAttributes = paramCount;
        this.numberOfClusters = numberOfClusters;
        this.parallelism = parallel;
        this.sampleFrequency = samplefrequency;
        this.interval = interval;
        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaClusters = new ConcurrentLinkedQueue<Clustering>(); // contain cluster centers

        try {
            this.processTaskBuilder = new StreamingClusteringTaskBuilder(this.maxEvents,
                    this.numberOfAttributes, this.numberOfClusters, this.parallelism,
                    this.sampleFrequency, this.interval, this.cepEvents, this.samoaClusters);
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Fail to Initiate the Streaming clustering"
                    + " task", e);
        }
    }

    public void addEvents(double[] eventData) {
        cepEvents.add(eventData);
    }

    public Object[] getOutput() {
        Object[] output;
        if (!samoaClusters.isEmpty()) {
            output = new Object[numberOfClusters];
            Clustering clusters = samoaClusters.poll();
            for (int i = 0; i < numberOfClusters; i++) {
                Cluster cluster = clusters.get(i);
                StringBuilder centerStr = new StringBuilder("[");
                double[] center = cluster.getCenter();
                centerStr.append(center[0]);
                for (int j = 1; j < numberOfAttributes; j++) {
                    centerStr.append(",").append(center[j]);
                }
                centerStr.append("]");
                output[i] = centerStr.toString();
            }
        } else {
            output = null;
        }
        return output;
    }
}
