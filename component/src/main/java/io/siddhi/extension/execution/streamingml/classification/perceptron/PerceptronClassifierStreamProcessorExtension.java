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

package io.siddhi.extension.execution.streamingml.classification.perceptron;

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
import io.siddhi.extension.execution.streamingml.classification.perceptron.util.PerceptronModel;
import io.siddhi.extension.execution.streamingml.classification.perceptron.util.PerceptronModelsHolder;
import io.siddhi.extension.execution.streamingml.util.CoreUtils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Predict using a linear binary classification Perceptron model built via
 * {@link PerceptronClassifierStreamProcessorExtension}
 */
@Extension(
        name = "perceptronClassifier",
        namespace = "streamingml",
        description = "This extension predicts using a linear binary classification Perceptron model.",
        parameters = {
                @Parameter(name = "model.name",
                        description = "The name of the model to be used.",
                        type = {DataType.STRING}),
                @Parameter(name = "model.bias",
                        description = "The bias of the Perceptron algorithm.",
                        type = {DataType.DOUBLE}, optional = true, defaultValue = "0.0"),
                @Parameter(name = "model.threshold",
                        description = "The threshold that separates the two classes. The value specified must be " +
                                "between zero and one.",
                        type = {DataType.DOUBLE}, optional = true, defaultValue = "0.5"),
                @Parameter(name = "model.feature",
                        description = "The features of the model that need to be attributes of the stream.",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"model.name", "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.bias", "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.threshold", "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.bias", "model.threshold",
                        "model.feature", "..."})
        },
        returnAttributes = {
                @ReturnAttribute(name = "prediction",
                        description = "The predicted value (`true/false`)",
                        type = {DataType.BOOL}),
                @ReturnAttribute(name = "confidenceLevel",
                        description = "The probability of the prediction",
                        type = {DataType.DOUBLE})
        },
        examples = {
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#streamingml:perceptronClassifier('model1',0.0,0.5, attribute_0, " +
                                "attribute_1, attribute_2, attribute_3) \n" +
                                "insert all events into OutputStream;",
                        description = "This query uses a Perceptron model named `model1` with a `0.0` bias and a " +
                                "`0.5` threshold learning rate to predict the label of the feature vector " +
                                "represented by `attribute_0`, `attribute_1`, `attribute_2`, and `attribute_3`. " +
                                "The predicted label (`true/false`) is emitted to the `OutputStream` stream" +
                                "along with the prediction confidence level(probability) and the feature vector. " +
                                "As a result, the OutputStream stream is defined as follows: " +
                                "(attribute_0 double, attribute_1 double, attribute_2" +
                                " double, attribute_3 double, prediction bool, confidenceLevel double)."
                ),
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#streamingml:perceptronClassifier('model1',0.0, attribute_0, " +
                                "attribute_1, attribute_2, attribute_3) \n" +
                                "insert all events into OutputStream;",
                        description = "This query uses a Perceptron model named `model1` with a `0.0` bias to predict" +
                                " the label of the feature vector represented by `attribute_0`, `attribute_1`, " +
                                "`attribute_2`, and `attribute_3`. The prediction(`true/false`) is emitted to the " +
                                "`OutputStream`stream along with the prediction confidence level(probability) and " +
                                "the feature. " +
                                "As a result, the OutputStream stream is defined as follows: " +
                                "(attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, " +
                                "prediction bool, confidenceLevel double)."
                ),
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#streamingml:perceptronClassifier(`model1`, attribute_0, attribute_1, " +
                                "attribute_2) \n" +
                                "insert all events into OutputStream;",
                        description = "This query uses a Perceptron model named `model1` with a default 0.0 bias" +
                                " to predict the label of the feature vector represented by `attribute_0`, " +
                                "`attribute_1`, and `attribute_2`. The predicted probability is emitted to the " +
                                "OutputStream stream along with the feature vector. As a result, the OutputStream is " +
                                "defined as follows: " +
                                "(attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, " +
                                "prediction bool, confidenceLevel double)."
                )
        }
)
public class PerceptronClassifierStreamProcessorExtension extends StreamProcessor<State> {

    private static Logger logger = LogManager.getLogger(PerceptronClassifierStreamProcessorExtension.class);
    private String modelName;
    private int numberOfFeatures;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();

    private ArrayList<Attribute> attributes;

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
        PerceptronModel model;
        String modelPrefix;
        double bias = -1, threshold = -1;
        // maxNumberOfFeatures = number of attributes - label attribute
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size();

        if (attributeExpressionLength >= 2) {
            if (attributeExpressionLength > 3 + maxNumberOfFeatures) {
                throw new SiddhiAppCreationException(String.format("Invalid number of parameters for " +
                        "streamingml:perceptronClassifier. This Stream Processor requires at most %s " + "parameters," +
                        " namely, model.name, model.bias, model.threshold, model.features but found %s " +
                        "parameters", 3 + maxNumberOfFeatures, attributeExpressionLength));
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
                // bias
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
                    bias = (double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found for the model.bias " +
                            "argument. Expected: " + Attribute.Type.DOUBLE + " but found: " +
                            attributeExpressionExecutors[1].getReturnType().toString());
                }
                // 3rd param
                if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                    // threshold
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE) {
                        threshold = (double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                                .getValue();
                        if (threshold <= 0 || threshold >= 1) {
                            throw new SiddhiAppCreationException("Invalid parameter value found for the model" + "" +
                                    ".threshold argument. Expected a value between 0 & 1, but found: " + threshold);
                        }
                    } else {
                        throw new SiddhiAppCreationException("Invalid parameter type found for the model.threshold " +
                                "" + "argument. Expected: " + Attribute.Type.DOUBLE + " but found: " +
                                attributeExpressionExecutors[2].getReturnType().toString());
                    }
                    // set number of features
                    numberOfFeatures = attributeExpressionLength - 3;
                    // feature variables
                    featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                            attributeExpressionExecutors, 3, numberOfFeatures);

                } else if (attributeExpressionExecutors[2] instanceof VariableExpressionExecutor) {
                    // set number of features
                    numberOfFeatures = attributeExpressionLength - 2;
                    // feature variables
                    featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                            attributeExpressionExecutors, 2, numberOfFeatures);
                } else {
                    throw new SiddhiAppCreationException("3rd Parameter must either be a constant (model.threshold)" +
                            "" + " or an attribute of the stream (model.features), but found a " +
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
                throw new SiddhiAppCreationException("2nd Parameter must either be a constant (model.bias) or " +
                        "an attribute of the stream (model.features), but found a " + attributeExpressionExecutors[1]
                        .getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppCreationException(String.format("Invalid number of parameters [%s] for " +
                    "streamingml:perceptronClassifier", attributeExpressionLength));
        }

        model = PerceptronModelsHolder.getInstance().getPerceptronModel(modelName);

        if (model != null) {
            if (bias != -1) {
                model.setBias(bias);
            }
            if (threshold != -1) {
                model.setThreshold(threshold);
            }
            if (model.getFeatureSize() != -1) {
                // validate the model
                if (numberOfFeatures != model.getFeatureSize()) {
                    throw new SiddhiAppCreationException(
                            String.format("Model [%s] expects %s features, but the streamingml:perceptronClassifier " +
                                            "specifies %s features",
                                    modelPrefix, model.getFeatureSize(), numberOfFeatures));
                }
            }
        } else {
            throw new SiddhiAppCreationException(String.format("Model [%s] needs to initialized "
                    + "prior to be used with streamingml:perceptronClassifier. "
                    + "Perform streamingml:updatePerceptronClassifier process first.", modelName));

        }

        attributes = new ArrayList<>();
        attributes.add(new Attribute("prediction", Attribute.Type.BOOL));
        attributes.add(new Attribute("confidenceLevel", Attribute.Type.DOUBLE));
        return null;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributes;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        PerceptronModelsHolder.getInstance().deletePerceptronModel(modelName);
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
                    // attributes cannot ever be any other type than double or int as we've validated the query at init
                    features[i] = ((Number) featureVariableExpressionExecutors.get(i).execute(event)).doubleValue();
                }

                Object[] data = PerceptronModelsHolder.getInstance().getPerceptronModel(modelName).classify(features);
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
