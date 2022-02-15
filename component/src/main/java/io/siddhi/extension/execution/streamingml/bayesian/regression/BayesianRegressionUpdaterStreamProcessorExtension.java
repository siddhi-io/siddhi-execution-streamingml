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
import io.siddhi.extension.execution.streamingml.bayesian.util.BayesianModel;
import io.siddhi.extension.execution.streamingml.bayesian.util.LinearRegression;
import io.siddhi.extension.execution.streamingml.util.CoreUtils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Bayesian regression.
 */

@Extension(
        name = "updateBayesianRegression",
        namespace = "streamingml",
        description = "This extension builds/updates a linear Bayesian regression model. " +
                "This extension uses an improved version of stochastic variational inference.",
        parameters = {
                @Parameter(
                        name = "model.name",
                        description = "The name of the model to be built.",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "model.target",
                        description = "The target attribute (dependant variable) of the input stream.",
                        type = {DataType.INT, DataType.DOUBLE, DataType.LONG, DataType.FLOAT},
                        dynamic = true
                ),
                @Parameter(
                        name = "model.samples",
                        description = "Number of samples used to construct the gradients.",
                        type = {DataType.INT}, optional = true, defaultValue = "1"
                ),
                @Parameter(
                        name = "model.optimizer",
                        description = "The type of optimization used",
                        type = {DataType.STRING}, optional = true, defaultValue = "ADAM"
                ),
                @Parameter(
                        name = "learning.rate",
                        description = "The learning rate of the updater",
                        type = {DataType.DOUBLE}, optional = true, defaultValue = "0.05"
                ),
                @Parameter(
                        name = "model.feature",
                        description = "Features of the model that need to be attributes of the stream.",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG},
                        dynamic = true
                )
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"model.name", "model.target",
                        "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.target", "model.samples",
                        "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.target", "model.optimizer",
                        "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.target", "learning.rate",
                        "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.target", "model.samples",
                        "model.optimizer", "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.target", "model.samples",
                        "learning.rate", "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.target", "model.optimizer",
                        "learning.rate", "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"model.name", "model.target", "model.samples",
                        "model.optimizer", "learning.rate", "model.feature", "..."})
        },
        returnAttributes = {
                @ReturnAttribute(name = "loss", description = " loss of the model.",
                        type = {DataType.DOUBLE})
        },
        examples = {
                @Example(syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double," +
                        " attribute_3 double, attribute_4 double );\n\n" +
                        "from StreamA#streamingml:updateBayesianRegression('model1', attribute_4, " +
                        "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                        "insert all events into outputStream;",
                        description = "This query builds/updates a Bayesian Linear regression model " +
                                "named `model1` using `attribute_0`, `attribute_1`, " +
                                "`attribute_2`, and `attribute_3` as features, and `attribute_4` as the label. " +
                                "Updated weights of the model are emitted to the OutputStream stream."),
                @Example(syntax = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 double," +
                        " attribute_3 double, attribute_4 double );\n\n" +
                        "from StreamA#streamingml:updateBayesianRegression('model1', attribute_4, 2, 'NADAM', 0.01, " +
                        "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                        "insert all events into outputStream;",
                        description = "This query builds/updates a Bayesian Linear regression model " +
                                "named `model1` with a `0.01` learning rate using `attribute_0`, `attribute_1`, " +
                                "`attribute_2`, and `attribute_3` as features, and `attribute_4` as the label. " +
                                "Updated weights of the model are emitted to the OutputStream stream. " +
                                "This model draws two samples during monte-carlo integration and uses NADAM optimizer.")


        }

)
public class BayesianRegressionUpdaterStreamProcessorExtension
        extends StreamProcessor<BayesianRegressionUpdaterStreamProcessorExtension.ExtensionState> {

    private static final Logger logger = LogManager.getLogger(BayesianRegressionUpdaterStreamProcessorExtension.class);
    private String modelName;
    private int numberOfFeatures;
    private VariableExpressionExecutor targetVariableExpressionExecutor;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();

    private ArrayList<Attribute> attributes;

    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent,
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

        double learningRate = -1;
        int nSamples = -1;
        BayesianModel.OptimizerType opimizerName = null;

        // maxNumberOfFeatures = number of attributes - label attribute
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size() - 1;
        int minNumberOfAttributes = 3;
        int maxNumberOfHyperParameters = 5;

        if (attributeExpressionLength >= minNumberOfAttributes) {
            if (attributeExpressionLength > maxNumberOfHyperParameters + maxNumberOfFeatures) {
                throw new SiddhiAppCreationException(String.format("Invalid number of parameters for " +
                        "streamingml:updateBayesianRegression. This Stream Processor requires at most %s " +
                        "parameters, namely, model.name, model.target, model.samples[optional], " +
                        "model.optimizer[optional], " + "learning.rate[optional], model.features. but found %s " +
                        "parameters", maxNumberOfHyperParameters + maxNumberOfFeatures, attributeExpressionLength));
            }
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    modelPrefix = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
                    // model name = user given name + siddhi app name
                    modelName = modelPrefix + "." + siddhiAppName;

                    if (LinearRegressionModelHolder.getInstance().getLinearRegressionMap().containsKey(modelName)) {
                        throw new SiddhiAppCreationException("A model already exists with name the " + modelPrefix +
                                ". Use a different value for model.name argument.");
                    }
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
                targetVariableExpressionExecutor = (VariableExpressionExecutor) this.attributeExpressionExecutors[1];
                // label attribute should be double or integer
                Attribute.Type targetAttributeType = inputDefinition.getAttributeType(targetVariableExpressionExecutor
                        .getAttribute().getName());
                if (!CoreUtils.isNumeric(targetAttributeType)) {
                    throw new SiddhiAppCreationException(String.format("[model.target] %s in " +
                                    "updateBayesianRegression should be a numeric. But found %s",
                            targetVariableExpressionExecutor.getAttribute().getName(), targetAttributeType.name()));
                }
            } else {
                throw new SiddhiAppCreationException("model.target attribute in updateBayesianRegression should "
                        + "be a variable, but found a " + this.attributeExpressionExecutors[1].getClass()
                        .getCanonicalName());
            }


            int start = 2, index = 2;
            // setting hyper parameters
            while (attributeExpressionExecutors[index] instanceof ConstantExpressionExecutor) {
                // number of samples from the gradient
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
                        attributeExpressionExecutors[index].getClass().getCanonicalName());
            }


        } else {
            throw new SiddhiAppCreationException(String.format("Invalid number of parameters [%s] for " +
                            "streamingml:updateBayesianRegression. Expect at least %s parameters",
                    attributeExpressionLength, minNumberOfAttributes));
        }


        // if no model exists, then create a new model
        model = new LinearRegression();
        LinearRegressionModelHolder.getInstance().addLinearRegressionModel(modelName, model);

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
                        "streamingml:updateBayesianRegression specifies %s features", modelPrefix, model
                        .getNumFeatures(), numberOfFeatures));
            }
        } else {
            model.setNumFeatures(numberOfFeatures);
            model.initiateModel();
        }

        attributes = new ArrayList<>();
        attributes.add(new Attribute("loss", Attribute.Type.DOUBLE));

        return () -> new ExtensionState(modelName);
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk,
                           Processor processor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater,
                           ExtensionState extensionState) {
        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                StreamEvent event = complexEventChunk.next();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Event received; Model name: %s Event:%s", modelName, event));
                }

                double[] target = new double[]{((Number) targetVariableExpressionExecutor.execute(event))
                        .doubleValue()};
                double[] features = new double[numberOfFeatures];
                for (int i = 0; i < numberOfFeatures; i++) {
                    // attributes cannot ever be any other type than int or double as we've validated the query at init
                    features[i] = ((Number) featureVariableExpressionExecutors.get(i).execute(event)).doubleValue();
                }
                double[] loss = LinearRegressionModelHolder.getInstance().getLinearRegressionModel(modelName).
                        update(features, target);

                Object[] data = new Object[1];
                data[0] = loss[0];
                complexEventPopulater.populateComplexEvent(event, data);
            }
        }
        nextProcessor.process(complexEventChunk);
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
        LinearRegressionModelHolder.getInstance().deleteLinearRegressionModel(modelName);
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    static class ExtensionState extends State {

        private static final String KEY_BAYSEIAN_REGRESSION_MODEL = "BayesianRegressionModel";
        private final Map<String, Object> state;
        private final String modelName;

        private ExtensionState(String modelName) {
            state = new HashMap<>();
            this.modelName = modelName;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            state.put(KEY_BAYSEIAN_REGRESSION_MODEL,
                    LinearRegressionModelHolder.getInstance().getClonedLinearRegressionModel(modelName));
            return state;
        }

        @Override
        public void restore(Map<String, Object> map) {
            LinearRegression model = (LinearRegression) state.get(KEY_BAYSEIAN_REGRESSION_MODEL);
            model.initiateModel();
            LinearRegressionModelHolder.getInstance().addLinearRegressionModel(modelName, model);
        }
    }
}
