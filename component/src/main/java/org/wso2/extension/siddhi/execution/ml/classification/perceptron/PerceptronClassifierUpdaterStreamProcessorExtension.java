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

package org.wso2.extension.siddhi.execution.ml.classification.perceptron;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.ml.classification.perceptron.util.PerceptronModel;
import org.wso2.extension.siddhi.execution.ml.classification.perceptron.util.PerceptronModelsHolder;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
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
 * Build or update a linear binary classification Perceptron model and emit weights of the features in the order of the
 * attributes.
 */
@Extension(
        name = "updatePerceptronClassifier",
        namespace = "ml",
        description = "Build/update a linear binary classification Perceptron model.",
        parameters = {
                @Parameter(name = "model.name",
                        description = "The name of the model to be built/updated.",
                        type = {DataType.STRING}),
                @Parameter(name = "label",
                        description = "The attribute of the label or the class of the dataset.",
                        type = {DataType.BOOL, DataType.STRING}),
                @Parameter(name = "learning.rate",
                        description = "The learning rate of the Perceptron algorithm.",
                        type = {DataType.DOUBLE}, optional = true, defaultValue = "0.1")
        },
        examples = {
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double, attribute_4 string );\n" +
                                "\n" +
                                "from StreamA#ml:updatePerceptronClassifier('model1', attribute_4, 0.01) \n" +
                                "insert all events into outputStream;",
                        description = "A Perceptron model with the name 'model1' will be built/updated with a 0.01 " +
                                "learning rate using attribute_0, attribute_1, attribute_2, attribute_3 as features " +
                                "and attribute_4 as the label. Updated weights of the model will be emitted to the " +
                                "outputStream."
                ),
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double, attribute_4 string );\n" +
                                "\n" +
                                "from StreamA#ml:updatePerceptronClassifier('model1', attribute_4) \n" +
                                "insert all events into outputStream;",
                        description = "A Perceptron model with the name 'model1' will be built/updated with a default" +
                                " 0.1 learning rate using attribute_0, attribute_1, attribute_2, attribute_3 as " +
                                "features and attribute_4 as the label. Updated weights of the model will be emitted " +
                                "to the outputStream."
                )
        }
)
public class PerceptronClassifierUpdaterStreamProcessorExtension extends StreamProcessor {

    private static Logger logger = Logger.getLogger(PerceptronClassifierUpdaterStreamProcessorExtension.class);
    private String modelName;
    private int numberOfFeatures;
    private VariableExpressionExecutor labelVariableExpressionExecutor;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        String siddhiAppName = siddhiAppContext.getName();
        PerceptronModel model;

        if (attributeExpressionExecutors.length >= 2) {
            if (attributeExpressionExecutors.length > 3) {
                throw new SiddhiAppValidationException(String.format("Invalid number of parameters for " +
                        "ml:updatePerceptronClassifier. This Stream Processor requires at most 3 parameters, namely, model" +
                        ".name, label, learning.rate, but found %s parameters", attributeExpressionExecutors.length));
            }
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    modelName = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
                    // model name = user given name + siddhi app name
                    modelName = modelName + "." + siddhiAppName;
                    model = PerceptronModelsHolder.getInstance().getPerceptronModel(modelName);
                    if (model == null) {
                        model = new PerceptronModel();
                        PerceptronModelsHolder.getInstance().addPerceptronModel(modelName, model);
                    }
                } else {
                    throw new SiddhiAppValidationException(
                            "Invalid parameter type found for the model.name argument, " + "required "
                                    + Attribute.Type.STRING
                                    + " but found " + attributeExpressionExecutors[0].
                                    getReturnType().toString());
                }
            } else {
                throw new SiddhiAppValidationException(
                        "Parameter model.name must be a constant"
                                + " but found "
                                + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }

            if (this.attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
                labelVariableExpressionExecutor = (VariableExpressionExecutor) this.attributeExpressionExecutors[1];
            } else {
                throw new SiddhiAppValidationException(
                        "label attribute in updatePerceptronClassifier should be a variable. but found a " +
                                this.attributeExpressionExecutors[1].getClass().getCanonicalName()
                );
            }

            if (attributeExpressionExecutors.length == 3) {
                if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE) {
                        double learningRate = (double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                                .getValue();
                        model.setLearningRate(learningRate);
                    } else {
                        throw new SiddhiAppValidationException(
                                "Invalid parameter type found for the learning.rate argument. Expected: "
                                        + Attribute.Type.DOUBLE + " but found: " + attributeExpressionExecutors[2].
                                        getReturnType().toString());
                    }
                } else {
                    throw new SiddhiAppValidationException(
                            "Parameter learning.rate must be a constant,"
                                    + " but found a "
                                    + attributeExpressionExecutors[2].getClass().getCanonicalName());
                }
            }

            // label attribute should be bool or string types
            Attribute.Type labelAttributeType = inputDefinition.getAttributeType(labelVariableExpressionExecutor
                    .getAttribute()
                    .getName());
            if (!(labelAttributeType.equals(Attribute.Type.BOOL) || labelAttributeType.equals(Attribute.Type.STRING))) {
                throw new SiddhiAppValidationException(String.format("[label attribute] %s in " +
                                "updatePerceptronClassifier should be either a %s or a %s (true/false). but found %s",
                        labelVariableExpressionExecutor.getAttribute().getName(), Attribute.Type.BOOL, Attribute
                                .Type.STRING, labelAttributeType.name()));
            }

            // other attributes should be double type.
            for (Attribute attribute : inputDefinition.getAttributeList()) {
                if (!attribute.getName().equals(labelVariableExpressionExecutor.getAttribute()
                        .getName()) && attribute.getType() != Attribute.Type.DOUBLE) {
                    throw new SiddhiAppValidationException(String.format("Attributes other than the [label attribute]" +
                            " %s in updatePerceptronClassifier should be of type %s. But there's an attribute called " +
                            "%s of type %s", labelVariableExpressionExecutor.getAttribute().getName(), Attribute.Type
                            .DOUBLE, attribute.getName(), attribute.getType().name()));
                }
            }

        } else {
            throw new SiddhiAppValidationException(String.format("Invalid number of parameters for " +
                    "ml:updatePerceptronClassifier. This Stream Processor requires at least 2 parameters, namely, " +
                    "model.name, label, but found %s parameters", attributeExpressionExecutors.length));
        }

        // number of weights = number of attributes - label attribute
        numberOfFeatures = inputDefinition.getAttributeList().size() - 1;
        List<Attribute> attributes = new ArrayList<Attribute>(numberOfFeatures);
        for (int i = 0; i < numberOfFeatures; i++) {
            attributes.add(new Attribute("weight_" + i, Attribute.Type.DOUBLE));
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
                    logger.debug(String.format("Event received; Model name:%s :%s Event:%s", modelName, event));
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
                } else if (labelObj instanceof Boolean) {
                    label = Boolean.toString((boolean) labelObj);
                } else {
                    throw new SiddhiAppRuntimeException(String.format("Detected attribute type of the label " +
                            "is %s. Expected attribute type is either String (true/false) or Bool. Note: Perceptron " +
                            "classifier can be used only for binary classification problems.", labelObj.getClass()
                            .getName()));
                }
                int labelAttributeIndex = labelVariableExpressionExecutor.getPosition()[3];

                double[] features = new double[numberOfFeatures];
                int j = 0;
                for (int i = 0; i < numberOfFeatures + 1; i++) {
                    if (i != labelAttributeIndex) {
                        // attributes cannot ever be any other type than double as we've validated the query at init
                        features[j] = (double) event.getOutputData()[i];
                        j++;
                    }
                }

                double[] weights = PerceptronModelsHolder.getInstance().getPerceptronModel(modelName).update(Boolean
                        .parseBoolean(label), features);
                // convert weights to object[]
                Object[] data = new Object[weights.length];
                for (int i = 0; i < weights.length; i++) {
                    data[i] = weights[i];
                }
                // create a new stream event
                StreamEvent newEvent = new StreamEvent(0, 0, numberOfFeatures);
                newEvent.setOutputData(data);
                streamEventChunk.insertBeforeCurrent(newEvent);

                streamEventChunk.remove();
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        // delete model
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put("PerceptronModelsMap", PerceptronModelsHolder.getInstance().getClonedPerceptronModelMap());
        return currentState;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        PerceptronModelsHolder.getInstance().setPerceptronModelMap((Map<String, PerceptronModel>) state.get
                ("PerceptronModelsMap"));
    }
}
