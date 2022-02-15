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
package io.siddhi.extension.execution.streamingml.bayesian.regression;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
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
import io.siddhi.extension.execution.streamingml.bayesian.regression.util.LinearRegressionModelHolder;
import io.siddhi.extension.execution.streamingml.bayesian.util.LinearRegression;
import io.siddhi.extension.execution.streamingml.util.CoreUtils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Predict using a Bayesian regression model built via
 * {@link BayesianRegressionUpdaterStreamProcessorExtension}.
 */


@Extension(
        name = "bayesianRegression",
        namespace = "streamingml",
        description = "This extension predicts using a Bayesian linear regression model." +
                "Bayesian linear regression allows determining the uncertainty of each prediction by estimating " +
                "the full-predictive distribution",
        parameters = {
                @Parameter(name = "model.name",
                        description = "The name of the model to be used",
                        type = {DataType.STRING}),
                @Parameter(name = "prediction.samples",
                        description = "The number of samples to be drawn to estimate the prediction",
                        type = {DataType.INT}, optional = true, defaultValue = "1000"),
                @Parameter(name = "model.feature",
                        description = "The features of the model that need to be attributes of the stream",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"model.name", "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "prediction.samples", "model.feature", "..."})
        },
        returnAttributes = {
                @ReturnAttribute(name = "prediction",
                        description = "The predicted value (double)",
                        type = {DataType.DOUBLE}),
                @ReturnAttribute(name = "confidence",
                        description = "Inverse of the standard deviation of the predictive distribution",
                        type = {DataType.DOUBLE}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#streamingml:bayesianRegression('model1', attribute_0, " +
                                "attribute_1, attribute_2, attribute_3) \n" +
                                "insert all events into OutputStream;",
                        description = "This query uses a Bayesian regression model named `model1` to predict " +
                                "the label of the feature vector represented by " +
                                "`attribute_0`, `attribute_1`, `attribute_2`, and `attribute_3`. " +
                                "The predicted value is emitted to the `OutputStream` stream" +
                                "along with the prediction confidence (std of predictive distribution) " +
                                "and the feature vector. As a result, the OutputStream stream is defined as follows: " +
                                "(attribute_0 double, attribute_1 double, attribute_2" +
                                " double, attribute_3 double, prediction double, confidence double)."
                )
        }
)


public class BayesianRegressionStreamProcessorExtension extends StreamProcessor<State> {

    private static final Logger logger = LogManager.getLogger(BayesianRegressionStreamProcessorExtension.class);
    private String modelName;
    private int numberOfFeatures;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();

    private ArrayList<Attribute> attributes;

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent,
                                       AbstractDefinition abstractDefinition,
                                       ExpressionExecutor[] attributeExpressionExecutors,
                                       ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean b,
                                       boolean b1,
                                       SiddhiQueryContext siddhiQueryContext) {
        String siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
        LinearRegression model;
        String modelPrefix;

        int predictionSamples = -1;
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size();
        int minNumberOfAttributes = 2;
        int maxNumberOfHyperParameters = 2;

        if (attributeExpressionLength >= minNumberOfAttributes) {
            if (attributeExpressionLength > maxNumberOfHyperParameters + maxNumberOfFeatures) {
                throw new SiddhiAppCreationException(String.format("Invalid number of parameters for " +
                        "streamingml:bayesianRegression. This Stream Processor requires at most %s " + "parameters," +
                        " namely, model.name, prediction.samples[optional], model.features but found %s " +
                        "parameters", maxNumberOfHyperParameters + maxNumberOfFeatures,
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
                    // set number of features
                    numberOfFeatures = attributeExpressionLength - 2;
                    // feature variables
                    featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                            attributeExpressionExecutors, 2, numberOfFeatures);
                } else {
                    throw new SiddhiAppCreationException("3rd Parameter must be an attribute of the " +
                            "stream (model.features), but found a " +
                            attributeExpressionExecutors[2].getClass().getCanonicalName());
                }
            } else if (attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
                // set number of features
                numberOfFeatures = attributeExpressionLength - 1;
                // feature values
                /*  extractAndValidateFeatures(inputDefinition, attributeExpressionExecutors, 1);*/
                featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                        attributeExpressionExecutors, 1, numberOfFeatures);
            } else {
                throw new SiddhiAppCreationException("2nd Parameter must either be a constant " +
                        "(prediction.samples) or an attribute of the stream (model.features), " +
                        "but found a " + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppCreationException(String.format("Invalid number of parameters [%s] for " +
                            "streamingml:bayesianRegression. Expect at least %s parameters",
                    attributeExpressionLength, minNumberOfAttributes));
        }

        model = LinearRegressionModelHolder.getInstance().getLinearRegressionModel(modelName);

        if (model != null) {
            if (predictionSamples != -1) {
                model.setPredictionSamples(predictionSamples);
            }
            if (model.getNumFeatures() != -1) {
                // validate the model
                if (numberOfFeatures != model.getNumFeatures()) {
                    throw new SiddhiAppCreationException(String.format("Model [%s] expects %s features, but the " +
                                    "streamingml:bayesianRegression specifies %s features",
                            modelPrefix, model.getNumFeatures(), numberOfFeatures));
                }
            }
        } else {
            throw new SiddhiAppCreationException(String.format("Model [%s] needs to initialized "
                    + "prior to be used with streamingml:bayesianRegression. "
                    + "Perform streamingml:updateBayesianRegression process first.", modelName));

        }

        attributes = new ArrayList<>();
        attributes.add(new Attribute("prediction", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("confidence", Attribute.Type.DOUBLE));

        return null;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributes;
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
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

                Object[] data = LinearRegressionModelHolder.getInstance().getLinearRegressionModel(modelName)
                        .predictWithStd(features);
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
