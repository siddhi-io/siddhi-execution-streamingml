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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.evaluation.ClusteringResultContentEvent;
import org.apache.samoa.learners.clusterers.ClusteringContentEvent;
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.moa.clusterers.clustream.WithKmeans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.execution.ml.samoa.utils.EvaluationProcessor;

import java.io.File;
import java.util.Queue;

/**
 * Streaming Clustering Evaluation Processor
 */
public class StreamingClusteringEvaluationProcessor extends EvaluationProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            StreamingClusteringEvaluationProcessor.class);
    private static final long serialVersionUID = 33332;

    String evaluationPoint;
    public Queue<Clustering> samoaClusters;
    public int numberOfClusters;
    private final int samplingFrequency;
    private final int decayHorizon;
    private final File dumpFile;

    StreamingClusteringEvaluationProcessor(StreamingClusteringEvaluationProcessor.Builder builder) {
        this.samplingFrequency = builder.samplingFrequency;
        this.dumpFile = builder.dumpFile;
        this.decayHorizon = builder.decayHorizon;

    }

    @Override
    public boolean process(ContentEvent event) {
        if (event instanceof ClusteringContentEvent) {
            LOGGER.info(event.getKey() + " " + evaluationPoint + "ClusteringContentEvent");
        } else if (event instanceof ClusteringResultContentEvent) {
            ClusteringResultContentEvent resultEvent = (ClusteringResultContentEvent) event;
            Clustering clustering = resultEvent.getClustering();

            Clustering kmeansClustering = WithKmeans.kMeans_rand(numberOfClusters, clustering);
            // Adding samoa Clusters into class
            samoaClusters.add(kmeansClustering);
        }
        return true;
    }

    @Override
    public void onCreate(int id) {
        this.processId = id;
        // logger.info("Creating PrequentialSourceProcessor with processId {}", processId);
    }

    @Override
    public Processor newProcessor(Processor p) {
        assert p instanceof StreamingClusteringEvaluationProcessor;
        StreamingClusteringEvaluationProcessor newEvaluator =
                (StreamingClusteringEvaluationProcessor) p;
        return newEvaluator;
    }

    public void setSamoaClusters(Queue<Clustering> samoaClusters) {
        this.samoaClusters = samoaClusters;
    }

    public void setNumberOfClusters(int numberOfClusters) {
        this.numberOfClusters = numberOfClusters;
    }

    /**
     * Builder Class
     */
    public static class Builder {
        private int samplingFrequency = 1000;
        private File dumpFile = null;
        private int decayHorizon = 1000;

        public Builder(int samplingFrequency) {
            this.samplingFrequency = samplingFrequency;
        }

        public Builder(StreamingClusteringEvaluationProcessor oldProcessor) {
            this.samplingFrequency = oldProcessor.samplingFrequency;
            this.dumpFile = oldProcessor.dumpFile;
            this.decayHorizon = oldProcessor.decayHorizon;
        }

        public StreamingClusteringEvaluationProcessor.Builder samplingFrequency(
                int samplingFrequency) {
            this.samplingFrequency = samplingFrequency;
            return this;
        }

        public StreamingClusteringEvaluationProcessor.Builder decayHorizon(int decayHorizon) {
            this.decayHorizon = decayHorizon;
            return this;
        }

        public StreamingClusteringEvaluationProcessor.Builder dumpFile(File file) {
            this.dumpFile = file;
            return this;
        }

        public StreamingClusteringEvaluationProcessor build() {
            return new StreamingClusteringEvaluationProcessor(this);
        }
    }

}
