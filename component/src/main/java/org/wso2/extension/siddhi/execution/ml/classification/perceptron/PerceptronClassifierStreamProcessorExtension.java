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
 * Predict using a linear binary classification Perceptron model built via
 * @{@link PerceptronClassifierUpdaterStreamProcessorExtension}
 */
@Extension(
        name = "perceptronClassifier",
        namespace = "ml",
        description = "Predict using a linear binary classification Perceptron model.",
        parameters = {
                @Parameter(name = "model.name",
                        description = "The name of the model to be used.",
                        type = {DataType.STRING}),
                @Parameter(name = "bias",
                        description = "The bias of the Perceptron algorithm.",
                        type = {DataType.DOUBLE}, defaultValue = "0.0"),
                @Parameter(name = "threshold",
                        description = "The threshold which separates the two classes. A value between 0 and 1.",
                        type = {DataType.DOUBLE}, defaultValue = "Output will be a probability.")
        },
        examples = {
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#ml:perceptronClassifier('model1',0.0,0.5) \n" +
                                "insert all events into outputStream;",
                        description = "A Perceptron model with the name 'model1' will be used with a 0.0 bias and a " +
                                "0.5 threshold learning rate to predict the label of the feature vector represented " +
                                "by attribute_0, attribute_1, attribute_2, attribute_3. Predicted label (true/false) " +
                                "along with the feature vector will be emitted to the outputStream. The outputStream " +
                                "will have following definition; (attribute_0 double, attribute_1 double, attribute_2" +
                                " double, attribute_3 double, prediction bool)."
                ),
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#ml:perceptronClassifier('model1',0.0) \n" +
                                "insert all events into outputStream;",
                        description = "A Perceptron model with the name 'model1' will be used with a 0.0 bias to " +
                                "predict the label of the feature vector represented by attribute_0, attribute_1, " +
                                "attribute_2, attribute_3. Predicted probability along with the feature vector will " +
                                "be emitted to the outputStream. The outputStream will have following definition; " +
                                "(attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, " +
                                "prediction double)."
                ),
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double, " +
                                "attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#ml:perceptronClassifier('model1') \n" +
                                "insert all events into outputStream;",
                        description = "A Perceptron model with the name 'model1' will be used with a default 0.0 bias" +
                                " to predict the label of the feature vector represented by attribute_0, attribute_1," +
                                " " +
                                "attribute_2, attribute_3. Predicted probability along with the feature vector will " +
                                "be emitted to the outputStream. The outputStream will have following definition; " +
                                "(attribute_0 double, attribute_1 double, attribute_2 double, attribute_3 double, " +
                                "prediction double)."
                )
        }
)
public class PerceptronClassifierStreamProcessorExtension extends StreamProcessor {

    private static Logger logger = Logger.getLogger(PerceptronClassifierStreamProcessorExtension.class);
    private PerceptronModel model;
    private String modelName;
    private int numberOfFeatures;
    private VariableExpressionExecutor labelVariableExpressionExecutor;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        String siddhiAppName = siddhiAppContext.getName();

        if (attributeExpressionExecutors.length >= 1) {
            if (attributeExpressionExecutors.length > 3) {
                throw new SiddhiAppValidationException(String.format("Invalid number of parameters for " +
                        "ml:perceptronClassifier. This Stream Processor requires at most 3 parameters, namely, model" +
                        ".name, bias, threshold, but found %s parameters", attributeExpressionExecutors.length));
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
                            "Invalid parameter type found for the model.name, " + "required "
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

            if (attributeExpressionExecutors.length == 2) {
                // 2nd attribute is bias
                if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
                        double bias = (double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                                .getValue();
                        model.setBias(bias);
                    } else {
                        throw new SiddhiAppValidationException(
                                "Invalid parameter type found for the bias argument. Expected: "
                                        + Attribute.Type.DOUBLE + " but found: " + attributeExpressionExecutors[1].
                                        getReturnType().toString());
                    }
                } else {
                    throw new SiddhiAppValidationException(
                            "Parameter bias must be a constant,"
                                    + " but found a "
                                    + attributeExpressionExecutors[1].getClass().getCanonicalName());
                }
            }

            if (attributeExpressionExecutors.length == 3) {
                // 3rd attribute is threshold
                if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE) {
                        double threshold = (double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                                .getValue();
                        model.setThreshold(threshold);
                    } else {
                        throw new SiddhiAppValidationException(
                                "Invalid parameter type found for the threshold argument. Expected: "
                                        + Attribute.Type.DOUBLE + " but found: " + attributeExpressionExecutors[2].
                                        getReturnType().toString());
                    }
                } else {
                    throw new SiddhiAppValidationException(
                            "Parameter threshold must be a constant,"
                                    + " but found a "
                                    + attributeExpressionExecutors[2].getClass().getCanonicalName());
                }
            }

            // all attributes should be of type double.
            for (Attribute attribute : inputDefinition.getAttributeList()) {
                if (attribute.getType() != Attribute.Type.DOUBLE) {
                    throw new SiddhiAppValidationException(String.format("Attributes " +
                            "of the stream where ml:perceptronClassifier is applied should be of type %s. But there's" +
                            " an attribute called " +
                            "%s of type %s", Attribute.Type
                            .DOUBLE, attribute.getName(), attribute.getType().name()));
                }
            }

        } else {
            throw new SiddhiAppValidationException(String.format("Invalid number of parameters for " +
                    "ml:perceptronClassifier. This Stream Processor requires at least one parameter, namely, model" +
                    ".name, but found %s parameters", attributeExpressionExecutors.length));
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

                double[] weights = model.update(Boolean.parseBoolean(label), features);
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
                ("PerceptronModelsHolder"));
    }
}
