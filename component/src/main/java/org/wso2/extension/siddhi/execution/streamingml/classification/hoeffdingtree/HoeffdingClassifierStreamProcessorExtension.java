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
package org.wso2.extension.siddhi.execution.streamingml.classification.hoeffdingtree;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.streamingml.classification.hoeffdingtree.util.AdaptiveHoeffdingModelsHolder;
import org.wso2.extension.siddhi.execution.streamingml.classification.hoeffdingtree.util.AdaptiveHoeffdingTreeModel;
import org.wso2.extension.siddhi.execution.streamingml.util.CoreUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
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
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Predict using a Hoeffding Adaptive Tree model.
 * built via @{@link HoeffdingClassifierStreamProcessorExtension}
 */
@Extension(
        name = "hoeffdingPredict",
        namespace = "streamingml",
        description = "Performs classification with Hoeffiding Adaptive Tree monitoring " +
                "Concept drift with ADWIN ",
        parameters = {
                @Parameter(name = "model.name",
                        description = "The name of the model to be used for prediction.",
                        type = {DataType.STRING})
        },
        returnAttributes = {
                @ReturnAttribute(name = "prediction",
                        description = "Predicted class label",
                        type = {DataType.STRING}),
                @ReturnAttribute(name = "confidenceLevel",
                        description = "Probability of the prediction",
                        type = {DataType.DOUBLE})
        },
        examples = {
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, " +
                                "attribute_2 double, attribute_3 double);\n" +
                                "\n" +
                                "from StreamA#streamingml:hoeffdingPredict('model1', " +
                                " attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                                "select attribute_0, attribute_1, attribute_2, attribute_3, " +
                                "prediction, predictionConfidence insert into outputStream;",
                        description = "A Hoeffding Tree model with the name 'model1' will be used " +
                                "to predict the label of the feature vector represented " +
                                "by attribute_0, attribute_1, attribute_2, attribute_3. " +
                                "Predicted label (String/Bool) along with the Prediction Confidence " +
                                "and the feature vector will be emitted to the outputStream. " +
                                "The outputStream will have following definition; " +
                                "(attribute_0 double, attribute_1 double, attribute_2" +
                                " double, attribute_3 double, prediction string, " +
                                "confidenceLevel double)."
                )
        }
)
public class HoeffdingClassifierStreamProcessorExtension extends StreamProcessor {
    private static final Logger logger = Logger
            .getLogger(HoeffdingClassifierStreamProcessorExtension.class);
    private int numberOfAttributes;
    private int parameterPosition = 1;
    private ExecutorService executorService;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();
    private String modelName;


    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition,
                                   ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        executorService = siddhiAppContext.getExecutorService();
        String siddhiAppName = siddhiAppContext.getName();
        String modelPrefix;
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size();
        if (attributeExpressionExecutors.length >= 3) {
            if (attributeExpressionLength > 1 + maxNumberOfFeatures) {
                throw new SiddhiAppValidationException(String.format("Invalid number of parameters for " +
                        "streamingml:hoeffdingClassifier. This Stream Processor requires at most %s " + "parameters," +
                        " namely, model.name, model.features but found %s " +
                        "parameters", 1 + maxNumberOfFeatures, attributeExpressionLength));
            }
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    modelPrefix = (String) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[0])
                            .getValue();
                    // model name = user given name + siddhi app name
                    modelName = modelPrefix + "." + siddhiAppName;
                } else {
                    throw new SiddhiAppValidationException(
                            "Invalid parameter type found for the model.name argument, "
                                    + "required " + Attribute.Type.STRING
                                    + " but found " + attributeExpressionExecutors[0].
                                    getReturnType().toString());
                }
                //numberOfAttributes = attributeExpressionLength - 1;
                numberOfAttributes = maxNumberOfFeatures;

                featureVariableExpressionExecutors = CoreUtils
                        .extractAndValidateFeatures(inputDefinition, attributeExpressionExecutors,
                                parameterPosition, numberOfAttributes + 1);

                AdaptiveHoeffdingTreeModel model = AdaptiveHoeffdingModelsHolder.getInstance()
                        .getHoeffdingModel(modelName);
                if (model == null) {
                    logger.debug("Creating new model Hoeffding Adaptive Tree model with the name, " + modelName);
                    model = new AdaptiveHoeffdingTreeModel();
                    AdaptiveHoeffdingModelsHolder.getInstance().addHoeffdingModel(modelName, model);
                }
                if (model.getStreamHeader() != null) {
                    // validate the model
                    if (numberOfAttributes != model.getNumberOfAttributes() - 1) {
                        // clean the model
                        logger.debug("Deleting the model " + modelName);
                        AdaptiveHoeffdingModelsHolder.getInstance().deleteHoeffdingModel(modelName);
                        throw new SiddhiAppValidationException(String.format("Model [%s] " +
                                        "expects %s features, but the " +
                                        "streamingml:hoeffdingClassifier specifies %s features"
                                , modelPrefix, model.getNumberOfAttributes()
                                , numberOfAttributes));
                    }
                } else {
                    model.init(numberOfAttributes);
                }
            } else {
                throw new SiddhiAppValidationException("Parameter model.name must be a constant but found " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException(String.format("Invalid number of " +
                    "parameters for ml:hoeffdingPredict. This Stream Processor requires " +
                    "at least 3 parameters, namely, model.name and at least 2 feature_attributes," +
                    " but found %s parameters", attributeExpressionExecutors.length));
        }

        //set attributes for Output Stream
        List<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(new Attribute("prediction", Attribute.Type.STRING));
        attributes.add(new Attribute("confidenceLevel", Attribute.Type.DOUBLE));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor
            processor, StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[numberOfAttributes];

                    // Set feature_attributes
                    for (int i = 0; i < numberOfAttributes; i++) {
                        cepEvent[i] = ((Number) featureVariableExpressionExecutors.get(i)
                                .execute(complexEvent)).doubleValue();
                    }

                    AdaptiveHoeffdingTreeModel model = AdaptiveHoeffdingModelsHolder.getInstance()
                            .getHoeffdingModel(modelName);
                    Object[] outputData = model.getPrediction(cepEvent);
                    int indexPredict = (int) outputData[0];
                    outputData[0] = model.getClasses().get(indexPredict);
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                }
            }
            nextProcessor.process(streamEventChunk);
        }
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
        currentState.put("AdaptiveHoeffdingModelsMap", AdaptiveHoeffdingModelsHolder.
                getInstance().getClonedHoeffdingModelMap());
        return currentState;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        AdaptiveHoeffdingModelsHolder.getInstance().
                setHoeffdingModelMap((Map<String, AdaptiveHoeffdingTreeModel>) state.get
                        ("AdaptiveHoeffdingModelsMap"));
    }

}
