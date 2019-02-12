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
package org.wso2.extension.siddhi.execution.streamingml.bayesian.classification;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.streamingml.bayesian.classification.util.SoftmaxRegressionModelHolder;
import org.wso2.extension.siddhi.execution.streamingml.bayesian.util.BayesianModel;
import org.wso2.extension.siddhi.execution.streamingml.bayesian.util.SoftmaxRegression;
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
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Bayesian classification using multivariate logistic regression.
 */

@Extension(
        name = "updateBayesianClassification",
        namespace = "streamingml",
        description = "This extension train a Bayesian multivariate logistic regression model. We can use this model " +
                "for multi-class classification. This extension uses an improved version of " +
                "stochastic variational inference.",
        parameters = {
                @Parameter(
                        name = "model.name",
                        description = "The name of the model to be built.",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "no.of.classes",
                        description = "Number of classes to be classified by the model.",
                        type = {DataType.DOUBLE, DataType.INT}
                ),
                @Parameter(
                        name = "model.target",
                        description = "The target attribute (dependant variable) of the input stream.",
                        type = {DataType.DOUBLE, DataType.INT}
                ),
                @Parameter(
                        name = "model.samples",
                        description = "Number of samples used to construct the gradients.",
                        type = {DataType.INT}, optional = true, defaultValue = "1"
                ),
                @Parameter(
                        name = "model.optimizer",
                        description = "The type of optimization used.",
                        type = {DataType.STRING}, optional = true, defaultValue = "ADAM"
                ),
                @Parameter(
                        name = "learning.rate",
                        description = "The learning rate of the updater.",
                        type = {DataType.DOUBLE}, optional = true, defaultValue = "0.05"
                ),
                @Parameter(
                        name = "model.features",
                        description = "Features of the model that need to be attributes of the stream.",
                        type = {DataType.DOUBLE}
                )
        },
        returnAttributes = {
                @ReturnAttribute(name = "loss", description = "Weight of the <feature" +
                        ".name> of the " + "model.", type = {DataType.DOUBLE})
        },
        examples = {
                @Example(syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double," +
                        " attribute_3 double, attribute_4 string );\n\n" +
                        "from StreamA#streamingml:updateBayesianClassification('model1', 3, attribute_4," +
                        "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                        "insert all events into outputStream;",
                        description = "This query builds/updates a Bayesian Softmax regression model " +
                                "named `model1` using `attribute_0`, `attribute_1`, " +
                                "`attribute_2`, and `attribute_3` as features, and `attribute_4` as the label. " +
                                "Updated weights of the model are emitted to the OutputStream stream. " +
                                "This models is capable of classifying 3 classes."),

                @Example(syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double," +
                        " attribute_3 double, attribute_4 string );\n\n" +
                        "from StreamA#streamingml:updateBayesianClassification('model1', 2, attribute_4, 0.01, " +
                        "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                        "insert all events into outputStream;",
                        description = "This query builds/updates a Bayesian Softmax regression model " +
                                "named `model1` with a `0.01` learning rate using `attribute_0`, `attribute_1`, " +
                                "`attribute_2`, and `attribute_3` as features, and `attribute_4` as the label. " +
                                "Updated weights of the model are emitted to the OutputStream stream. " +
                                "This models act as a binary classifier.")

        }

)
public class BayesianClassificationUpdaterStreamProcessorExtension extends StreamProcessor {

    private static Logger logger = Logger.getLogger(BayesianClassificationUpdaterStreamProcessorExtension.class);
    private String modelName;
    private int numberOfFeatures;
    private VariableExpressionExecutor targetVariableExpressionExecutor;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();


    /**
     * The initialization method for {@link StreamProcessor}, which will be called before other methods and validate
     * the all configuration and getting the initial values.
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the Function
     * @param siddhiAppContext             Siddhi app runtime context
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        String siddhiAppName = siddhiAppContext.getName();
        SoftmaxRegression model;
        String modelPrefix;

        double learningRate = -1;
        int nSamples = -1;
        int numberOfClasses = -1;
        BayesianModel.OptimizerType opimizerName = null;

        // maxNumberOfFeatures = number of attributes - label attribute
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size() - 1;
        int minNumberOfAttributes = 4;
        int maxNumberOfHyperParameters = 6;

        if (attributeExpressionLength >= minNumberOfAttributes) {
            if (attributeExpressionLength > maxNumberOfHyperParameters + maxNumberOfFeatures) {
                throw new SiddhiAppCreationException(String.format("Invalid number of parameters for " +
                        "streamingml:updateBayesianClassification. This Stream Processor requires at most %s " +
                        "parameters, namely, model.name, no.of.classes, model.target, model.samples[optional], " +
                        "model.optimizer[optional], " + "learning.rate[optional], model.features. but found %s " +
                        "parameters", maxNumberOfHyperParameters + maxNumberOfFeatures, attributeExpressionLength));
            }
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    modelPrefix = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
                    // model name = user given name + siddhi app name
                    modelName = modelPrefix + "." + siddhiAppName;

                    if (SoftmaxRegressionModelHolder.getInstance().getSoftmaxRegressionMap().containsKey(modelName)) {
                        throw new SiddhiAppCreationException("A model already exists with name the " + modelPrefix +
                                ". Use a different value for model.name argument.");
                    }

                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found for the model.name argument," +
                            " required " + Attribute.Type.STRING + " But found " + attributeExpressionExecutors[0].
                            getReturnType().toString());
                }
            } else {
                throw new SiddhiAppCreationException("Parameter model.name must be a constant." + " But found " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }

            // check number of classes
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    int val = (int) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                    if (val <= 1) {
                        throw new SiddhiAppCreationException(String.format("no.of.classes should be greater than 1."
                                + " But found %d", val));
                    } else {
                        numberOfClasses = val;
                    }
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found " +
                            "for the no.of.classes argument, required " + Attribute.Type.INT + " But found "
                            + attributeExpressionExecutors[1].getReturnType().toString());
                }
            } else {
                throw new SiddhiAppCreationException("Parameter no.of.classes must be a constant." + " But found " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[2] instanceof VariableExpressionExecutor) {
                targetVariableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[2];
                // label attribute should be double or integer
                Attribute.Type targetAttributeType = inputDefinition.getAttributeType(targetVariableExpressionExecutor
                        .getAttribute().getName());
                if (!CoreUtils.isLabelType(targetAttributeType)) {
                    throw new SiddhiAppCreationException(String.format("[model.target] %s in " +
                                    "updateBayesianClassification should be a STRING or BOOLEAN. But found %s",
                            targetVariableExpressionExecutor.getAttribute().getName(), targetAttributeType.name()));
                } else if (targetAttributeType == Attribute.Type.BOOL && numberOfClasses != 2) {
                    throw new SiddhiAppCreationException("no.of.classes should be 2, if the type of the attribute " +
                            "model.target is BOOLEAN. But found " + numberOfClasses);
                }
            } else {
                throw new SiddhiAppCreationException("model.target attribute in updateBayesianClassification should "
                        + "be a variable, but found a " + attributeExpressionExecutors[2].getClass()
                        .getCanonicalName());
            }

            int index = 3, start = 3;
            // setting hyper parameters
            while (attributeExpressionExecutors[index] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[index].getReturnType() == Attribute.Type.INT) {
                    if (index != start) {
                        throw new SiddhiAppCreationException(String.format("%dth parameter cannot be type of %s. " +
                                        "Only model.sample can be %s, which can be set as the %dth parameter.",
                                index, Attribute.Type.INT, Attribute.Type.INT, start));
                    }
                    int val = (int) ((ConstantExpressionExecutor) attributeExpressionExecutors[index])
                            .getValue();
                    if (val <= 0) {
                        throw new SiddhiAppCreationException(String.format("model.sample should be greater" +
                                " than zero." + "But found %d", val));
                    } else {
                        nSamples = val;
                        index += 1;
                    }
                } else if (attributeExpressionExecutors[index].getReturnType() == Attribute.Type.STRING) {
                    if (index > start + 1) {
                        throw new SiddhiAppCreationException(String.format("%dth parameter cannot be type of %s. " +
                                        "Only model.optimizer can be %s.",
                                index, Attribute.Type.STRING, Attribute.Type.STRING));
                    }
                    if (opimizerName != null) {
                        throw new SiddhiAppCreationException(String.format("%dth parameter cannot be type of %s. " +
                                        "Only model.optimizer can be %s, which is already set to %s.",
                                index, Attribute.Type.STRING, Attribute.Type.STRING, opimizerName));
                    }
                    // optimizer name
                    String val = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[index]).getValue();
                    try {
                        opimizerName = BayesianModel.OptimizerType.valueOf(val.toUpperCase(Locale.ENGLISH));
                        index += 1;
                    } catch (Exception ex) {
                        throw new SiddhiAppCreationException(String.format("model.optimizer should be one of " +
                                "%s. But found %s", Arrays.toString(BayesianModel.OptimizerType.values()), val));
                    }
                } else if (attributeExpressionExecutors[index].getReturnType() == Attribute.Type.DOUBLE) {
                    // learning rate
                    double val = (double) ((ConstantExpressionExecutor) attributeExpressionExecutors[index])
                            .getValue();
                    if (val <= 0) {
                        throw new SiddhiAppCreationException(String.format("learning.rate should " +
                                "be greater than zero. " + "But found %f", val));
                    } else {
                        learningRate = val;
                        index += 1;
                        break;
                    }
                } else {
                    throw new SiddhiAppCreationException(String.format("Invalid parameter type found. " +
                                    "Expected: %s or %s or %s. But found %s",
                            Attribute.Type.INT, Attribute.Type.STRING, Attribute.Type.DOUBLE,
                            attributeExpressionExecutors[2].getReturnType().toString()));
                }
            }
            if (attributeExpressionExecutors[index] instanceof VariableExpressionExecutor) {
                // set number of features
                numberOfFeatures = attributeExpressionLength - index;
                // feature values
                featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                        attributeExpressionExecutors, index, numberOfFeatures);
            } else {
                throw new SiddhiAppCreationException("Parameter " + index + " must either be a constant" +
                        " (hyperparameter) or an attribute of the stream (model" + ".features), but found a " +
                        attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

        } else {
            throw new SiddhiAppCreationException(String.format("Invalid number of parameters [%s] for " +
                            "streamingml:updateBayesianClassification. Expect at least %s parameters",
                    attributeExpressionLength, minNumberOfAttributes));
        }


        // if no model exists, then create a new model
        model = new SoftmaxRegression(numberOfClasses);
        SoftmaxRegressionModelHolder.getInstance().addSoftmaxRegressionModel(modelName, model);


        if (learningRate != -1) {
            logger.debug("set learning rate to : " + learningRate);
            model.setLearningRate(learningRate);
        }
        if (nSamples != -1) {
            logger.debug("set number of samples to : " + nSamples);
            model.setNumSamples(nSamples);
        }
        if (opimizerName != null) {
            logger.debug("set optimizer to : " + opimizerName);
            model.setOptimizerType(opimizerName);
        }

        if (model.getNumFeatures() != -1) {
            // validate the model
            if (numberOfFeatures != model.getNumFeatures()) {
                throw new SiddhiAppCreationException(String.format("Model [%s] expects %s features, but the " +
                        "streamingml:updateBayesianClassification specifies %s features", modelPrefix, model
                        .getNumFeatures(), numberOfFeatures));
            }
        } else {
            model.setNumFeatures(numberOfFeatures);
            model.initiateModel();
        }

        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("loss", Attribute.Type.DOUBLE));

        return attributes;
    }


    /**
     * Process events received by BayesianClassificationUpdateStreamingProcessorExtension.
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

                String target = targetVariableExpressionExecutor.execute(event).toString();
                double[] features = new double[numberOfFeatures];
                for (int i = 0; i < numberOfFeatures; i++) {
                    // attributes cannot ever be any other type than int or double as we've validated the query at init
                    features[i] = ((Number) featureVariableExpressionExecutors.get(i).execute(event)).doubleValue();
                }

                logger.debug(SoftmaxRegressionModelHolder.getInstance().getSoftmaxRegressionModel(modelName)
                        .evaluate(features, target));

                double[] loss = SoftmaxRegressionModelHolder.getInstance().getSoftmaxRegressionModel(modelName).
                        update(features, target);

                Object[] data = new Object[1];
                data[0] = loss[0];
                complexEventPopulater.populateComplexEvent(event, data);
            }
        }
        nextProcessor.process(streamEventChunk);
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
        SoftmaxRegressionModelHolder.getInstance().deleteSoftmaxRegressionModel(modelName);
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be.
     * persisted for reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an map
     */
    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put("SoftmaxRegressionModel", SoftmaxRegressionModelHolder.getInstance()
                .getClonedSoftmaxRegressionModel(modelName));
        return currentState;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing.
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the processing element as a map.
     *              This is the same map that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> state) {
        SoftmaxRegression model = (SoftmaxRegression) state.get("SoftmaxRegressionModel");
        model.initiateModel();
        SoftmaxRegressionModelHolder.getInstance().addSoftmaxRegressionModel(modelName, model);
    }

}
