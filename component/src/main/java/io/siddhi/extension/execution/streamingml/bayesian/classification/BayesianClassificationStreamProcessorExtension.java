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
package io.siddhi.extension.execution.streamingml.bayesian.classification;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.execution.streamingml.bayesian.classification.util.SoftmaxRegressionModelHolder;
import io.siddhi.extension.execution.streamingml.bayesian.util.SoftmaxRegression;
import io.siddhi.extension.execution.streamingml.util.CoreUtils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Predict using a Bayesian Softmax regression model built via
 * {@link BayesianClassificationUpdaterStreamProcessorExtension}.
 */

/**
 * This feature has been removed due to the isuue at https://github.com/siddhi-io/siddhi-execution-streamingml/issues/75
 */


public class BayesianClassificationStreamProcessorExtension extends StreamProcessor<State> {

    private static Logger logger = LogManager.getLogger(BayesianClassificationStreamProcessorExtension.class);
    private String modelName;
    private int numberOfFeatures;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();
    private SoftmaxRegression model;
    private List<Attribute> attributes;
    private int predictionSamples;
    private String modelPrefix;

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent,
                                       AbstractDefinition abstractDefinition,
                                       ExpressionExecutor[] expressionExecutors,
                                       ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean b,
                                       boolean b1,
                                       SiddhiQueryContext siddhiQueryContext) {
        String siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
        predictionSamples = -1;
        model = null;
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size();
        int minNumberOfAttributes = 2;
        int maxNumberOfHyperParameters = 2;

        if (attributeExpressionLength >= minNumberOfAttributes) {
            if (attributeExpressionLength > maxNumberOfHyperParameters + maxNumberOfFeatures) {
                throw new SiddhiAppCreationException(String.format("Invalid number of parameters for " +
                                "streamingml:bayesianClassification. This Stream Processor requires at most %s "
                                + "parameters, namely, model.name, prediction.samples[optional], model.features " +
                                "but found %s parameters", maxNumberOfHyperParameters + maxNumberOfFeatures,
                        attributeExpressionLength));
            }
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    modelPrefix = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
                    // model name = user given name + siddhi app name
                    modelName = modelPrefix + "." + siddhiAppName;
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found for the model.name argument," +
                            "" + " required " + Attribute.Type.STRING + " but found " +
                            attributeExpressionExecutors[0].getReturnType().toString());
                }
            } else {
                throw new SiddhiAppCreationException("Parameter model.name must be a constant but found " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }

            // 2nd param
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    int val = (int) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                    if (val <= 0) {
                        throw new SiddhiAppCreationException(String.format("Invalid parameter value found for the " +
                                "prediction.samples argument. Expected a value greater than zero, but found: %d", val));
                    }
                    predictionSamples = val;
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found for the prediction.samples " +
                            "argument. Expected: " + Attribute.Type.INT + " but found: " +
                            attributeExpressionExecutors[1].getReturnType().toString());
                }

                if (attributeExpressionExecutors[2] instanceof VariableExpressionExecutor) {
                    int numberOfHyperParameters = 2;
                    // set number of features
                    numberOfFeatures = attributeExpressionLength - numberOfHyperParameters;
                    // feature variables
                    featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                            attributeExpressionExecutors, numberOfHyperParameters, numberOfFeatures);
                } else {
                    throw new SiddhiAppCreationException("3rd Parameter must be an attribute of the " +
                            "stream (model.features), but found a " +
                            attributeExpressionExecutors[2].getClass().getCanonicalName());
                }
            } else if (attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
                int numberOfHyperParameters = 1;
                // set number of features
                numberOfFeatures = attributeExpressionLength - numberOfHyperParameters;
                // feature values
                /*  extractAndValidateFeatures(inputDefinition, attributeExpressionExecutors, 1);*/
                featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                        attributeExpressionExecutors, numberOfHyperParameters, numberOfFeatures);
            } else {
                throw new SiddhiAppCreationException("2nd Parameter must either be a constant " +
                        "(prediction.samples) or an attribute of the stream (model.features), " +
                        "but found a " + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppCreationException(String.format("Invalid number of parameters [%s] for " +
                            "streamingml:bayesianClassification. Expect at least %s parameters",
                    attributeExpressionLength, minNumberOfAttributes));
        }

        attributes = new ArrayList<>();
        attributes.add(new Attribute("prediction", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("confidence", Attribute.Type.DOUBLE));

        return null;
    }

    @Override
    public void start() {
        model = SoftmaxRegressionModelHolder.getInstance().getSoftmaxRegressionModel(modelName);
        if (model != null) {
            if (predictionSamples != -1) {
                model.setPredictionSamples(predictionSamples);
            }
            if (model.getNumFeatures() != -1) {
                // validate the model
                if (numberOfFeatures != model.getNumFeatures()) {
                    throw new SiddhiAppCreationException(String.format("Model [%s] expects %s features, but the " +
                                    "streamingml:bayesianClassification specifies %s features",
                            modelPrefix, model.getNumFeatures(), numberOfFeatures));
                }
            }
        } else {
            throw new SiddhiAppCreationException(String.format("Model [%s] needs to initialized "
                    + "prior to be used with streamingml:bayesianClassification. "
                    + "Perform streamingml:updateBayesianClassification process first.", modelName));

        }
    }

    @Override
    public void stop() {
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk,
                           Processor processor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater,
                           State state) {
        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                StreamEvent event = complexEventChunk.next();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Event received; Model name: %s Event:%s", modelName, event));
                }

                double[] features = new double[numberOfFeatures];
                for (int i = 0; i < numberOfFeatures; i++) {
                    // attributes cannot ever be any other type than double as we've validated the query at init
                    features[i] = ((Number) featureVariableExpressionExecutors.get(i).execute(event)).doubleValue();
                }

                Double[] predictWithStd = model.predictWithStd(features);

                // convert label index to label
                Object[] data = new Object[2];
                data[0] = model.getClassLabel((predictWithStd[0]).intValue());
                data[1] = predictWithStd[1];
                // If output has values, then add those values to output stream
                complexEventPopulater.populateComplexEvent(event, data);
            }
        }
        nextProcessor.process(complexEventChunk);
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }
}
