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

package org.wso2.extension.siddhi.execution.streamingml.classification.perceptron;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.streamingml.classification.perceptron.util.PerceptronModel;
import org.wso2.extension.siddhi.execution.streamingml.classification.perceptron.util.PerceptronModelsHolder;
import org.wso2.extension.siddhi.execution.streamingml.util.CoreUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Build or update a linear binary classification Perceptron model and emit the weights of the features in the order of
 * the attributes.
 */
@Extension(
        name = "updatePerceptronClassifier",
        namespace = "streamingml",
        description = "This extension builds/updates a linear binary classification Perceptron model.",
        parameters = {
                @Parameter(name = "model.name",
                        description = "The name of the model to be built/updated.",
                        type = {DataType.STRING}),
                @Parameter(name = "model.label",
                        description = "The attribute of the label or the class of the dataset.",
                        type = {DataType.BOOL, DataType.STRING}),
                @Parameter(name = "learning.rate",
                        description = "The learning rate of the Perceptron algorithm.",
                        type = {DataType.DOUBLE}, optional = true, defaultValue = "0.1"),
                @Parameter(name = "model.features",
                        description = "Features of the model that need to be attributes of the stream.",
                        type = {DataType.DOUBLE, DataType.INT})},
        returnAttributes = {
                @ReturnAttribute(name = "featureWeight", description = "Weight of the <feature" +
                        ".name> of the " + "model.", type = {DataType.DOUBLE})},
        examples = {
                @Example(syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double," +
                        " attribute_3 double, attribute_4 " + "string );\n\n" +
                        "from StreamA#streamingml:updatePerceptronClassifier('model1', attribute_4, 0.01, " +
                        "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                        "insert all events into outputStream;",
                        description = "This query builds/updates a Perceptron model named `model1` with a `0.01` " +
                                "learning rate using `attribute_0`, `attribute_1`, `attribute_2`, and `attribute_3` " +
                                "as features, and `attribute_4` as the label. Updated weights of the model " +
                                "are emitted to the OutputStream stream."),
                @Example(syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double," +
                        "attribute_3 double, attribute_4 string );\n\n " +
                        "from StreamA#streamingml:updatePerceptronClassifier('model1', attribute_4, attribute_0, " +
                        "attribute_1, attribute_2, attribute_3) \n" +
                        "insert all events into outputStream;",
                        description = "This query builds/updates a Perceptron model named `model1` with a default" +
                                " `0.1` learning rate using `attribute_0`, `attribute_1`, `attribute_2`, and " +
                                "`attribute_3` as features, and `attribute_4` as the label. The updated weights of " +
                                "the model are appended to the outputStream.")
        }
)
public class PerceptronClassifierUpdaterStreamProcessorExtension extends StreamProcessor {

    private static Logger logger = Logger.getLogger(PerceptronClassifierUpdaterStreamProcessorExtension.class);
    private String modelName;
    private int numberOfFeatures;
    private VariableExpressionExecutor labelVariableExpressionExecutor;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        String siddhiAppName = siddhiAppContext.getName();
        PerceptronModel model;
        String modelPrefix;
        double learningRate = -1;
        // maxNumberOfFeatures = number of attributes - label attribute
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size() - 1;

        if (attributeExpressionLength >= 3) {
            if (attributeExpressionLength > 3 + maxNumberOfFeatures) {
                throw new SiddhiAppCreationException(String.format("Invalid number of parameters for " +
                        "streamingml:updatePerceptronClassifier. This Stream Processor requires at most %s " +
                        "parameters, namely, model.name, model.label, learning.rate, model.features but found %s " +
                        "parameters", 3 + maxNumberOfFeatures, attributeExpressionLength));
            }
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    modelPrefix = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
                    // model name = user given name + siddhi app name
                    modelName = modelPrefix + "." + siddhiAppName;
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found for the model.name argument," +
                            " required " + Attribute.Type.STRING + " but found " + attributeExpressionExecutors[0].
                            getReturnType().toString());
                }
            } else {
                throw new SiddhiAppCreationException("Parameter model.name must be a constant" + " but found " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }

            if (this.attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
                labelVariableExpressionExecutor = (VariableExpressionExecutor) this.attributeExpressionExecutors[1];
                // label attribute should be bool or string types
                Attribute.Type labelAttributeType = inputDefinition.getAttributeType(labelVariableExpressionExecutor
                        .getAttribute().getName());
                if (!CoreUtils.isLabelType(labelAttributeType)) {
                    throw new SiddhiAppCreationException(String.format("[model.label] %s in " +
                                    "updatePerceptronClassifier should be either a %s or a %s (true/false). But found"
                                    + " %s", labelVariableExpressionExecutor.getAttribute().getName(),
                            Attribute.Type.BOOL, Attribute.Type.STRING, labelAttributeType.name()));
                }
            } else {
                throw new SiddhiAppCreationException("model.label attribute in updatePerceptronClassifier should "
                        + "be a variable, but found a " + this.attributeExpressionExecutors[1].getClass()
                        .getCanonicalName());
            }

            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                // learning rate
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE) {
                    learningRate = (double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                            .getValue();
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found for the learning.rate " +
                            "argument. Expected: " + Attribute.Type.DOUBLE + " but found: " +
                            attributeExpressionExecutors[2].getReturnType().toString());
                }

                // set number of features
                numberOfFeatures = attributeExpressionLength - 3;
                // feature values
                featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                        attributeExpressionExecutors, 3, numberOfFeatures);
            } else if (attributeExpressionExecutors[2] instanceof VariableExpressionExecutor) {
                // set number of features
                numberOfFeatures = attributeExpressionLength - 2;
                // feature values
                featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                        attributeExpressionExecutors, 2, numberOfFeatures);
            } else {
                throw new SiddhiAppCreationException("3rd Parameter must either be a constant (learning.rate) or "
                        + "an attribute of the stream (model" + ".features), but found a " +
                        attributeExpressionExecutors[2].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppCreationException(String.format("Invalid number of parameters [%s] for " +
                    "streamingml:updatePerceptronClassifier", attributeExpressionLength));
        }

        model = PerceptronModelsHolder.getInstance().getPerceptronModel(modelName);
        if (model == null) {
            model = new PerceptronModel();
            PerceptronModelsHolder.getInstance().addPerceptronModel(modelName, model);
        }
        if (learningRate != -1) {
            model.setLearningRate(learningRate);
        }
        if (model.getFeatureSize() != -1) {
            // validate the model
            if (numberOfFeatures != model.getFeatureSize()) {
                throw new SiddhiAppCreationException(String.format("Model [%s] expects %s features, but the " +
                        "streamingml:updatePerceptronClassifier specifies %s features", modelPrefix, model
                        .getFeatureSize(), numberOfFeatures));
            }
        } else {
            model.initWeights(numberOfFeatures);
        }

        List<Attribute> attributes = new ArrayList<>();
        for (int i = 0; i < numberOfFeatures; i++) {
            attributes.add(new Attribute(featureVariableExpressionExecutors.get(i).getAttribute().getName() +
                    ".weight", Attribute.Type.DOUBLE));
        }

        return attributes;
    }

    /**
     * Process events received by PerceptronClassifierUpdaterStreamProcessorExtension
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
                StreamEvent event = streamEventChunk.next();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Event received; Model name: %s Event:%s", modelName, event));
                }

                Object labelObj = labelVariableExpressionExecutor.execute(event);
                String label;
                if (labelObj instanceof String) {
                    label = (String) labelObj;
                    if (!(label.equalsIgnoreCase("true") || label.equalsIgnoreCase("false"))) {
                        throw new SiddhiAppRuntimeException(String.format("Detected attribute type of the label " +
                                "is String, but the value is not either true or false but %s. Note: Perceptron " +
                                "classifier can be used only for binary classification problems.", label));
                    }
                } else {
                    // should be a boolean as validated at init
                    label = Boolean.toString((boolean) labelObj);
                }

                double[] features = new double[numberOfFeatures];
                for (int i = 0; i < numberOfFeatures; i++) {
                    // attributes cannot ever be any other type than int or double as we've validated the query at init
                    features[i] = ((Number) featureVariableExpressionExecutors.get(i).execute(event)).doubleValue();
                }

                double[] weights = PerceptronModelsHolder.getInstance().getPerceptronModel(modelName).update(Boolean
                        .parseBoolean(label), features);
                // convert weights to object[]
                Object[] data = new Object[weights.length];
                for (int i = 0; i < weights.length; i++) {
                    data[i] = weights[i];
                }

                complexEventPopulater.populateComplexEvent(event, data);
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        PerceptronModelsHolder.getInstance().deletePerceptronModel(modelName);
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put("PerceptronModel", PerceptronModelsHolder.getInstance().getClonedPerceptronModel(modelName));
        return currentState;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        PerceptronModelsHolder.getInstance().addPerceptronModel(modelName, (PerceptronModel)
                state.get("PerceptronModel"));
    }
}
