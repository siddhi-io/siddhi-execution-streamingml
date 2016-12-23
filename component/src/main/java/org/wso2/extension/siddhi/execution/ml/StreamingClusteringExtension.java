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

package org.wso2.extension.siddhi.execution.ml;

import org.wso2.extension.siddhi.execution.ml.samoa.utils.clustering.StreamingClustering;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class StreamingClusteringExtension extends StreamProcessor {

    private int parameterPosition;
    private StreamingClustering streamingClustering;
    private ExecutorService executorService;

    /**
     * Initialize the StreamingClusteringExtension
     *
     * @param inputDefinition              Input Definition
     * @param attributeExpressionExecutors Array of AttributeExpressionExecutor
     * @param executionPlanContext         ExecutionPlanContext of Siddhi
     * @return clusterCenters, list of cluster centers injected by StreamingClusteringExtension
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        int maxEvents = -1;
        this.executorService = executionPlanContext.getExecutorService();
        int numberOfClusters;
        if (attributeExpressionExecutors.length >= 3) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                    Object constantObject = ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[0]).getValue();
                    numberOfClusters = (Integer) constantObject;
                    if (numberOfClusters < 2) {
                        throw new ExecutionPlanValidationException("Number of clusters must be" +
                                " greater than 1, but found " + numberOfClusters);
                    }
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for the"
                            + " first argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[0].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Number of clusters must be" +
                        " a constant but found variable value.");
            }

            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    maxEvents = (Integer) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[1]).getValue();
                    if (maxEvents < -1 ) {
                        throw new ExecutionPlanValidationException("Maximum number of events must be greater than" +
                                " or equal -1. (-1 = No limit), but found " + maxEvents);
                    }

                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for the" +
                            " second argument,required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[1].getReturnType().toString());
                }
                parameterPosition = 2;
                if (attributeExpressionExecutors.length == 3) {
                    throw new ExecutionPlanValidationException("At least 2 attributes required to" +
                            " clustering process, but found only 1 attribute");
                }
            } else {
                parameterPosition = 1;
            }
        } else {
            throw new ExecutionPlanValidationException("Invalid parameter count. At least required"
                    + " number of clusters and two attributes,but found " +
                    attributeExpressionExecutors.length + " parameters.");
        }
        int numberOfAttributes = attributeExpressionLength - parameterPosition;
        streamingClustering = new StreamingClustering(maxEvents, numberOfAttributes,
                numberOfClusters);

        // Add attributes
        List<Attribute> attributes = new ArrayList<Attribute>(numberOfClusters);
        for (int i = 0; i < numberOfClusters; i++) {
            attributes.add(new Attribute(("center" + i), Attribute.Type.STRING));
        }
        return attributes;
    }

    /**
     * Process events received by StreamingClusteringExtension
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[attributeExpressionLength - parameterPosition];

                    for (int i = parameterPosition; i < attributeExpressionLength; i++) {
                        cepEvent[i - parameterPosition] = ((Number) attributeExpressionExecutors[i].
                                execute(complexEvent)).doubleValue();
                    }
                    streamingClustering.addEvents(cepEvent);

                    Object[] outputData = streamingClustering.getOutput();
                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {
        executorService.execute(streamingClustering);
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[]{streamingClustering};
        // TODO: 12/20/16 check how to store this samoa app
    }

    @Override
    public void restoreState(Object[] state) {
        streamingClustering = (StreamingClustering) state[0];
    }
}
