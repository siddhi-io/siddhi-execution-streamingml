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

import org.wso2.extension.siddhi.execution.ml.samoa.utils.classification.StreamingClassification;
import org.wso2.extension.siddhi.execution.ml.samoa.utils.regression.StreamingRegression;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Extension(
        name = "regressionAMRules",
        namespace = "ml",
        description = "TBD",
        parameters = {
                @Parameter(name = "tbd",
                        description = "TBD",
                type = DataType.DOUBLE),

        },
        returnAttributes = @ReturnAttribute(
                name = "tbd",
                description = "Returns median of aggregated events",
                type = DataType.DOUBLE),
        examples = @Example(description = "TBD", syntax = "TBD")
)
public class StreamingRegressionExtension extends StreamProcessor {

    private int numberOfAttributes;
    private int parameterPosition;
    private StreamingRegression streamingRegression;
    private ExecutorService executorService;

    /**
     * Initialize the StreamingRegressionExtension
     *
     * @param abstractDefinition              Input Definition
     * @param expressionExecutors Array of AttributeExpressionExecutor
     * @param executionPlanContext         ExecutionPlanContext of Siddhi
     * @return attributes, list of attributes with prediction injected by StreamingRegressionExtension
     */
    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, ExecutionPlanContext executionPlanContext) {
        this.executorService = executionPlanContext.getExecutorService();
        int maxEvents = -1;
        int interval = 1000;
        int parallelism = 1;
        if (attributeExpressionExecutors.length >= 3) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                    numberOfAttributes = (Integer) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[0]).getValue();
                    if (numberOfAttributes < 2) {
                        throw new ExecutionPlanValidationException("Number of attributes must be greater than 1 but " +
                                "found " + numberOfAttributes);
                    }
                    if (numberOfAttributes + 4 < attributeExpressionExecutors.length) {
                        throw new ExecutionPlanValidationException("There is a inconsistency with number of " +
                                "attributes and entered attributes. Number of attributes should be greater than " +
                                numberOfAttributes + " or entered attributes should be change.");
                    }

                    for (int i = attributeExpressionExecutors.length - numberOfAttributes; i <
                            attributeExpressionExecutors.length; i++) {
                        if (!(attributeExpressionExecutors[i] instanceof VariableExpressionExecutor)) {
                            throw new ExecutionPlanValidationException("Parameter number " + (i + 1) + " is not an " +
                                    "attribute (VariableExpressionExecutor). Check " +
                                    "the number of attribute entered as an attribute set with number of attribute " +
                                    "configuration parameter");
                        }
                    }
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for the" +
                            " first argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[0].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Parameter count must be a constant " +
                        "( ConstantExpressionExecutor)and at" +
                        " least one configuration parameter required. streamingRegressionSamoa(parCount," +
                        "attribute_set) but found 0 configuration parameters.");
            }
            parameterPosition = attributeExpressionExecutors.length - numberOfAttributes;
            if (parameterPosition > 1) {
                if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                        interval = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for " +
                                "the second argument, required " + Attribute.Type.INT + " but found " +
                                attributeExpressionExecutors[1].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("Display interval  values must be a constant " +
                            "(ConstantExpressionExecutor) but found " +
                            "(" + attributeExpressionExecutors[1].getClass().getCanonicalName() + ") value.");
                }
            }

            if (parameterPosition > 2) {
                if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                        maxEvents = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                        if (maxEvents < -1) {
                            throw new ExecutionPlanValidationException("Maximum number of events must be greater than" +
                                    " or equal -1. (-1 = No limit), but found " + maxEvents);
                        }
                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                                " the third argument, required " + Attribute.Type.INT + " but found " +
                                attributeExpressionExecutors[2].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("The maximum number of events must be a constant " +
                            "(ConstantExpressionExecutor)but found " +
                            "(" + attributeExpressionExecutors[2].getClass().getCanonicalName() + ") value.");
                }
            }

            if (parameterPosition > 3) {
                if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                        parallelism = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[3])
                                .getValue();
                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                                " the fourth argument, required " + Attribute.Type.INT + " but found " +
                                attributeExpressionExecutors[3].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("Parallelism value must be a constant " +
                            "(ConstantExpressionExecutor) but found " +
                            "(" + attributeExpressionExecutors[3].getClass().getCanonicalName() + ") value.");
                }
            }
        } else {
            throw new ExecutionPlanValidationException("Number of parameter should be greater than 2  " +
                    "but found " + attributeExpressionExecutors.length);
        }

        streamingRegression = new StreamingRegression(maxEvents, interval, numberOfAttributes,
                parallelism);

        List<Attribute> attributes = new ArrayList<Attribute>(numberOfAttributes);
        for (int i = 0; i < numberOfAttributes - 1; i++) {
            attributes.add(new Attribute("att_" + i, Attribute.Type.DOUBLE));
        }
        attributes.add(new Attribute("prediction", Attribute.Type.DOUBLE));
        return attributes;
    }

    /**
     * Process events received by StreamingRegressionExtension
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                double[] cepEvent = new double[attributeExpressionLength - parameterPosition];
                for (int i = 0; i < numberOfAttributes; i++) {
                    cepEvent[i] = ((Number) attributeExpressionExecutors[i + parameterPosition].
                            execute(complexEvent)).doubleValue();
                }

                streamingRegression.addEvents(cepEvent);
                Object[] outputData = streamingRegression.getOutput();
                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {
        executorService.execute(streamingRegression);
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Map<String, Object> currentState() {
        return Collections.singletonMap("streamRegression", (Object) streamingRegression);
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        streamingRegression = (StreamingRegression) map.get("streamRegression");
    }
}

