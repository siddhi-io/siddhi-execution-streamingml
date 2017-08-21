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

package org.wso2.extension.siddhi.execution.ml.classification.perceptron;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.ml.classification.perceptron.util.PerceptronModel;
import org.wso2.extension.siddhi.execution.ml.classification.perceptron.util.PerceptronModelsHolder;
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
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Predict using a linear binary classification Perceptron model
 * built via @{@link PerceptronClassifierUpdaterStreamProcessorExtension}
 */
@Extension(
        name = "perceptronClassifier",
        namespace = "streamingml",
        description = "Predict using a linear binary classification Perceptron model.",
        parameters = {
                @Parameter(name = "model.name",
                        description = "The name of the model to be used.",
                        type = {DataType.STRING}),
                @Parameter(name = "model.bias",
                        description = "The bias of the Perceptron algorithm.",
                        type = {DataType.DOUBLE}, defaultValue = "0.0"),
                @Parameter(name = "model.threshold",
                        description = "The threshold which separates the two classes. " +
                                "A value between 0 and 1.",
                        type = {DataType.DOUBLE}, defaultValue = "Output will be a probability."),
                @Parameter(name = "model.features",
                        description = "Features of the model which should be " +
                                "attributes of the stream.",
                        type = {DataType.DOUBLE})
        },
        returnAttributes = {
                @ReturnAttribute(name = "prediction",
                        description = "Predicted value (true/false)",
                        type = {DataType.BOOL}),
                @ReturnAttribute(name = "confidenceLevel",
                        description = "Probability of the prediction",
                        type = {DataType.DOUBLE})
        },
        examples = {
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, " +
                                "attribute_2 double, attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#streamingml:perceptronClassifier('model1',0.0,0.5, " +
                                "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                                "insert all events into outputStream;",
                        description = "A Perceptron model with the name 'model1' will be used " +
                                "with a 0.0 bias and a 0.5 threshold learning rate to predict " +
                                "the label of the feature vector represented " +
                                "by attribute_0, attribute_1, attribute_2, attribute_3. " +
                                "Predicted label (true/false) along with the " +
                                "Prediction Confidence " +
                                "Level(probability) and the feature vector " +
                                "will be emitted to the outputStream. " +
                                "The outputStream will have following definition; " +
                                "(attribute_0 double, attribute_1 double, attribute_2" +
                                " double, attribute_3 double, prediction bool, " +
                                "confidenceLevel double)."
                ),
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, " +
                                "attribute_1 double, attribute_2 double, attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#streamingml:perceptronClassifier('model1',0.0, " +
                                "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                                "insert all events into outputStream;",
                        description = "A Perceptron model with the name 'model1' will be used " +
                                "with a 0.0 bias to predict the label of the feature vector " +
                                "represented by attribute_0, attribute_1, attribute_2, " +
                                "attribute_3. Prediction(true/false) along with the " +
                                "Prediction Confidence Level(probability) " +
                                "feature vector will be emitted to the outputStream. " +
                                "The outputStream will have following definition; " +
                                "(attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double, prediction bool, confidenceLevel double)."
                ),
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, " +
                                "attribute_1 double, attribute_2 double, attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#streamingml:perceptronClassifier('model1', " +
                                "attribute_0, attribute_1, attribute_2) \n" +
                                "insert all events into outputStream;",
                        description = "A Perceptron model with the name 'model1' will be used " +
                                "with a default 0.0 bias to predict the label of the " +
                                "feature vector represented by attribute_0, attribute_1," +
                                " attribute_2. Predicted probability along with " +
                                "the feature vector will be emitted to the outputStream. " +
                                "The outputStream will have following definition; " +
                                "(attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double, prediction bool, confidenceLevel double)."
                )
        }
)
public class PerceptronClassifierStreamProcessorExtension extends StreamProcessor {

    private static Logger logger =
            Logger.getLogger(PerceptronClassifierStreamProcessorExtension.class);
    private String modelName;
    private int numberOfFeatures;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        String siddhiAppName = siddhiAppContext.getName();
        PerceptronModel model;
        String modelPrefix;
        double bias = -1, threshold = -1;
        // maxNumberOfFeatures = number of attributes - label attribute
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size();

        if (attributeExpressionLength >= 2) {
            if (attributeExpressionLength > 3 + maxNumberOfFeatures) {
                throw new SiddhiAppValidationException(String.format("Invalid number of " +
                        "parameters for streamingml:perceptronClassifier. This Stream Processor " +
                        "requires at most %s " + "parameters, namely, model.name, model.bias, " +
                        "model.threshold, model.features but found %s " +
                        "parameters", 3 + maxNumberOfFeatures, attributeExpressionLength));
            }
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    modelPrefix = (String) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[0]).getValue();
                    // model name = user given name + siddhi app name
                    modelName = modelPrefix + "." + siddhiAppName;
                } else {
                    throw new SiddhiAppValidationException("Invalid parameter type found for the " +
                            "model.name argument," +
                            "" + " required " + Attribute.Type.STRING + " but found " +
                            attributeExpressionExecutors[0].getReturnType().toString());
                }
            } else {
                throw new SiddhiAppValidationException("Parameter model.name must be a " +
                        "constant but found " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }

            // 2nd param
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                // bias
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
                    bias = (double) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[1]).getValue();
                } else {
                    throw new SiddhiAppValidationException("Invalid parameter type found for the " +
                            "model.bias argument. Expected: " + Attribute.Type.DOUBLE +
                            " but found: " +
                            attributeExpressionExecutors[1].getReturnType().toString());
                }
                // 3rd param
                if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                    // threshold
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE) {
                        threshold = (double) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[2])
                                .getValue();
                        if (threshold <= 0 || threshold >= 1) {
                            throw new SiddhiAppValidationException("Invalid parameter value " +
                                    "found for the model threshold argument. Expected a " +
                                    "value between 0 & 1, but found: " + threshold);
                        }
                    } else {
                        throw new SiddhiAppValidationException("Invalid parameter type found " +
                                "for the model.threshold " +
                                "" + "argument. Expected: " + Attribute.Type.DOUBLE +
                                " but found: " +
                                attributeExpressionExecutors[2].getReturnType().toString());
                    }
                    // set number of features
                    numberOfFeatures = attributeExpressionLength - 3;
                    // feature variables
                    extractAndValidateFeatures(inputDefinition, attributeExpressionExecutors,
                            3);

                } else if (attributeExpressionExecutors[2] instanceof VariableExpressionExecutor) {
                    // set number of features
                    numberOfFeatures = attributeExpressionLength - 2;
                    // feature variables
                    extractAndValidateFeatures(inputDefinition, attributeExpressionExecutors,
                            2);
                } else {
                    throw new SiddhiAppValidationException("3rd Parameter must either be a " +
                            "constant (model.threshold)" +
                            "" + " or an attribute of the stream (model.features), but found a " +
                            attributeExpressionExecutors[2].getClass().getCanonicalName());
                }
            } else if (attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
                // set number of features
                numberOfFeatures = attributeExpressionLength - 1;
                // feature values
                extractAndValidateFeatures(inputDefinition, attributeExpressionExecutors,
                        1);
            } else {
                throw new SiddhiAppValidationException("2nd Parameter must either be a constant " +
                        "(model.bias) or " +
                        "an attribute of the stream (model.features), but found a " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException(String.format("Invalid number of " +
                            "parameters [%s] for streamingml:perceptronClassifier"
                    , attributeExpressionLength));
        }

        model = PerceptronModelsHolder.getInstance().getPerceptronModel(modelName);
        if (model == null) {
            model = new PerceptronModel();
            PerceptronModelsHolder.getInstance().addPerceptronModel(modelName, model);
        }
        if (bias != -1) {
            model.setBias(bias);
        }
        if (threshold != -1) {
            model.setThreshold(threshold);
        }
        if (model.getFeatureSize() != -1) {
            // validate the model
            if (numberOfFeatures != model.getFeatureSize()) {
                // clean the model
                PerceptronModelsHolder.getInstance().deletePerceptronModel(modelName);
                throw new SiddhiAppValidationException(String.format("Model [%s] expects %s " +
                                "features, but the streamingml:perceptronClassifier " +
                                "specifies %s features", modelPrefix, model.getFeatureSize()
                        , numberOfFeatures));
            }
        } else {
            model.initWeights(numberOfFeatures);
        }

        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("prediction", Attribute.Type.BOOL));
        attributes.add(new Attribute("confidenceLevel", Attribute.Type.DOUBLE));

        return attributes;
    }

    private void extractAndValidateFeatures(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, int startIndex) {
        // feature values start
        for (int i = startIndex; i < attributeExpressionLength; i++) {
            if (attributeExpressionExecutors[i] instanceof VariableExpressionExecutor) {
                featureVariableExpressionExecutors.add((VariableExpressionExecutor)
                        attributeExpressionExecutors[i]);
                // other attributes should be double type.
                String attributeName = ((VariableExpressionExecutor)
                        attributeExpressionExecutors[i]).getAttribute().getName();
                Attribute.Type featureAttributeType = inputDefinition.
                        getAttributeType(attributeName);
                if (!(featureAttributeType == Attribute.Type.DOUBLE ||
                        featureAttributeType == Attribute.Type.INT)) {
                    throw new SiddhiAppValidationException(String.format("model.features in " +
                                    "perceptronClassifier should be of type %s or %s. But there's an "
                                    + "attribute" + " called " + "%s of type " +
                                    "%s", Attribute.Type.DOUBLE, Attribute.Type.INT, attributeName,
                            featureAttributeType.name()));
                }
            } else {
                throw new SiddhiAppValidationException("Parameter[" + (i + 1) + "] of " +
                        "perceptronClassifier must be an attribute present in the stream, " +
                        "but found a " + attributeExpressionExecutors[i]
                        .getClass().getCanonicalName());
            }
        }
    }

    /**
     * Process events received by PerceptronClassifierUpdaterStreamProcessorExtension
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need
     *                              to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage
     *                              or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater
                                   complexEventPopulater) {

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Event received; Model name: %s Event:%s",
                            modelName, event));
                }

                double[] features = new double[numberOfFeatures];
                for (int i = 0; i < numberOfFeatures; i++) {
                    // attributes cannot ever be any other type than double as we've
                    // validated the query at init
                    features[i] = (double) featureVariableExpressionExecutors.get(i).execute(event);
                }

                Object[] data = PerceptronModelsHolder.getInstance()
                        .getPerceptronModel(modelName).classify(features);


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
        return new HashMap<>();
    }

    @Override
    public void restoreState(Map<String, Object> state) {
    }
}
