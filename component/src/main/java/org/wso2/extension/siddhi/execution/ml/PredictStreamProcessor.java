/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.execution.ml;


import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.log4j.Logger;
import org.wso2.carbon.ml.core.exceptions.MLInputAdapterException;
import org.wso2.carbon.ml.core.exceptions.MLModelHandlerException;
import org.wso2.carbon.ml.core.factories.AlgorithmType;
import org.wso2.carbon.ml.core.h2o.POJOPredictor;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
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
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * PredictStreamProcessor performs prediction
 */
@Extension(
        name = "predict",
        namespace = "ml",
        description = "Performs prediction",
        parameters =
                { // TODO: 6/16/17 To be Decided
                },
        examples = {
                @Example(
                        syntax = "TBD",
                        description = "TBD"
                )
        }
)
public class PredictStreamProcessor extends StreamProcessor {
    private static final Logger log = Logger.getLogger(PredictStreamProcessor.class);
    private ModelHandler[] modelHandlers;
    private String[] modelStorageLocations;
    private String responseVariable;
    private static final String anomalyPrediction = "prediction";
    private String algorithmClass;
    private String outputType;
    private double percentileValue;
    private boolean isAnomalyDetection;
    private boolean attributeSelectionAvailable;
    private Map<Integer, int[]> attributeIndexMap; // <feature-index,
    // [event-array-type][attribute-index]> pairs
    private POJOPredictor[] pojoPredictor;
    private boolean deeplearningWithoutH2O;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        while (streamEventChunk.hasNext()) {

            StreamEvent event = streamEventChunk.next();
            String[] featureValues = new String[attributeIndexMap.size()];

            for (Map.Entry<Integer, int[]> entry : attributeIndexMap.entrySet()) {
                int featureIndex = entry.getKey();
                int[] attributeIndexArray = entry.getValue();
                Object dataValue = null;
                switch (attributeIndexArray[2]) {
                case 0:
                    dataValue = event.getBeforeWindowData()[attributeIndexArray[3]];
                    break;
                case 2:
                    dataValue = event.getOutputData()[attributeIndexArray[3]];
                    break;
                default:
                    throw new SiddhiAppRuntimeException("Undefined index " + attributeIndexArray[2]);
                    // TODO: 6/20/17 throw more meaningful error
                }
                featureValues[featureIndex] = String.valueOf(dataValue);
            }

            try {
                Object[] predictionResults = new Object[modelHandlers.length];
                Object predictionResult = null;

                if (AlgorithmType.CLASSIFICATION.getValue().equals(algorithmClass)) {
                    for (int i = 0; i < modelHandlers.length; i++) {
                        predictionResults[i] = modelHandlers[i].predict(featureValues,
                                outputType);
                    }
                    // Gets the majority vote
                    predictionResult = ObjectUtils.mode(predictionResults);
                } else if (AlgorithmType.NUMERICAL_PREDICTION.getValue().equals(
                        algorithmClass)) {
                    double sum = 0;
                    for (int i = 0; i < modelHandlers.length; i++) {
                        sum += Double.parseDouble(modelHandlers[i].predict(featureValues,
                                outputType).toString());
                    }
                    // Gets the average value of predictions
                    predictionResult = sum / modelHandlers.length;
                } else if (AlgorithmType.ANOMALY_DETECTION.getValue().equals(algorithmClass)) {
                    for (int i = 0; i < modelHandlers.length; i++) {
                        predictionResults[i] = modelHandlers[i].predict(featureValues,
                                outputType, percentileValue);
                    }
                    // Gets the majority vote
                    predictionResult = ObjectUtils.mode(predictionResults);

                } else if (AlgorithmType.DEEPLEARNING.getValue().equals(algorithmClass)) {
                    // if H2O cluster is not available
                    if (deeplearningWithoutH2O) {
                        for (int i = 0; i < modelHandlers.length; i++) {
                            predictionResults[i] = modelHandlers[i].predict(featureValues,
                                    outputType,
                                    pojoPredictor[i]);
                        }
                        // Gets the majority vote
                        predictionResult = ObjectUtils.mode(predictionResults);
                    } else {
                        for (int i = 0; i < modelHandlers.length; i++) {
                            predictionResults[i] = modelHandlers[i].predict(featureValues,
                                    outputType);
                        }
                        // Gets the majority vote
                        predictionResult = ObjectUtils.mode(predictionResults);
                    }
                } else {
                    String msg = String.format(
                            "Error while predicting. Prediction is not supported for " +
                                    "the algorithm class %s. ",
                            algorithmClass);
                    throw new SiddhiAppRuntimeException(msg);
                }

                Object[] output = new Object[] { predictionResult };
                complexEventPopulater.populateComplexEvent(event, output);
            } catch (MLModelHandlerException e) {
                log.error("Error while predicting", e);
                throw new SiddhiAppRuntimeException("Error while predicting", e);
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        if (attributeExpressionExecutors.length < 2) {
            throw new SiddhiAppValidationException("ML model storage locations and response " +
                    "variable type have not "
                    + "been defined as the first two parameters");
        } else if (attributeExpressionExecutors.length == 2) {
            attributeSelectionAvailable = false; // model-storage-location, data-type
        } else {
            attributeSelectionAvailable = true; // model-storage-location, data-type,
            // stream-attributes list
        }

        // model-storage-location
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            Object constantObj = ((ConstantExpressionExecutor)
                    attributeExpressionExecutors[0]).getValue();
            String allPaths = (String) constantObj;
            modelStorageLocations = allPaths.split(",");
        } else {
            throw new SiddhiAppValidationException(
                    "ML model storage-location has not been defined as the first parameter");
        }

        // data-type
        Attribute.Type outputDatatype;
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).
                    getValue();
            outputType = (String) constantObj;
            outputDatatype = getOutputAttributeType(outputType);
        } else {
            throw new SiddhiAppValidationException(
                    "Response variable type has not been defined as the second parameter");
        }

        modelHandlers = new ModelHandler[modelStorageLocations.length];
        for (int i = 0; i < modelStorageLocations.length; i++) {
            try {
                modelHandlers[i] = new ModelHandler(modelStorageLocations[i]);
            } catch (ClassNotFoundException e) {
                logError(i, e);
            } catch (URISyntaxException e) {
                logError(i, e);
            } catch (MLInputAdapterException e) {
                logError(i, e);
            } catch (IOException e) {
                logError(i, e);
            }
        }

        // Validate algorithm classes
        // All models should have the same algorithm class.
        // When put into a set, the size of the set should be 1
        HashSet<String> algorithmClasses = new HashSet<String>();
        for (int i = 0; i < modelHandlers.length; i++) {
            algorithmClasses.add(modelHandlers[i].getAlgorithmClass());
        }
        if (algorithmClasses.size() > 1) {
            throw new SiddhiAppRuntimeException("Algorithm classes are not equal");
        }
        algorithmClass = modelHandlers[0].getAlgorithmClass();

        // Validate features
        // All models should have same features.
        // When put into a set, the size of the set should be 1
        HashSet<Map<String, Integer>> features = new HashSet<Map<String, Integer>>();
        for (int i = 0; i < modelHandlers.length; i++) {
            features.add(modelHandlers[i].getFeatures());
        }
        if (features.size() > 1) {
            throw new SiddhiAppRuntimeException("Features in models are not equal");
        }

        if (AlgorithmType.ANOMALY_DETECTION.getValue().equals(algorithmClass)) {
            isAnomalyDetection = true;
        }

        if (!isAnomalyDetection) {
            // Validate response variables
            // All models should have the same response variable.
            // When put into a set, the size of the set should be 1
            HashSet<String> responseVariables = new HashSet<String>();
            for (int i = 0; i < modelStorageLocations.length; i++) {
                responseVariables.add(modelHandlers[i].getResponseVariable());
            }
            if (responseVariables.size() > 1) {
                throw new SiddhiAppCreationException("Response variables of models are not equal");
            }
            responseVariable = modelHandlers[0].getResponseVariable();

        } else {
            if (attributeExpressionExecutors.length == 3) {
                attributeSelectionAvailable = false; // model-storage-location, data-type
            } else {
                attributeSelectionAvailable = true; // model-storage-location, data-type,
                // stream-attributes list
            }

            // checking the percentile value
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).
                        getValue();
                percentileValue = (Double) constantObj;

            } else {
                throw new SiddhiAppValidationException("percentile value has not been defined as " +
                        "the third parameter");
            }

            return Arrays.asList(new Attribute(anomalyPrediction, outputDatatype));
        }

        if (AlgorithmType.DEEPLEARNING.getValue().equals(algorithmClass)) {
            if (modelHandlers[0].getMlModel().getModel() != null) {
                deeplearningWithoutH2O = false;
            } else {
                deeplearningWithoutH2O = true;
                pojoPredictor = new POJOPredictor[modelHandlers.length];
                for (int i = 0; i < modelHandlers.length; i++) {
                    try {
                        pojoPredictor[i] = new POJOPredictor(modelHandlers[i].getMlModel(),
                                modelStorageLocations[i]);
                    } catch (MLModelHandlerException e) {
                        throw new SiddhiAppRuntimeException(
                                "Failed to initialize the POJO predictor of the model " +
                                        modelStorageLocations[i], e);
                    }
                }
            }
        }

        return Arrays.asList(new Attribute(responseVariable, outputDatatype));
    }

    @Override
    public void start() {
        try {
            populateFeatureAttributeMapping();
        } catch (SiddhiAppCreationException e) {
            log.error("Error while retrieving ML-models", e);
            throw new SiddhiAppCreationException("Error while retrieving ML-models" + "\n"
                    + e.getMessage());
        }
    }

    /**
     * Match the attribute index values of stream with feature index value of the model.
     *
     * @throws SiddhiAppCreationException
     */
    private void populateFeatureAttributeMapping() {
        attributeIndexMap = new HashMap<Integer, int[]>();
        Map<String, Integer> featureIndexMap = modelHandlers[0].getFeatures();
        List<Integer> newToOldIndicesList = modelHandlers[0].getNewToOldIndicesList();

        if (attributeSelectionAvailable) {
            for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
                if (expressionExecutor instanceof VariableExpressionExecutor) {
                    VariableExpressionExecutor variable = (VariableExpressionExecutor)
                            expressionExecutor;
                    String variableName = variable.getAttribute().getName();
                    if (featureIndexMap.get(variableName) != null) {
                        int featureIndex = featureIndexMap.get(variableName);
                        int newFeatureIndex = newToOldIndicesList.indexOf(featureIndex);
                        attributeIndexMap.put(newFeatureIndex, variable.getPosition());
                    } else {
                        throw new SiddhiAppCreationException(
                                "No matching feature name found in the models for the attribute : "
                                        + variableName);
                    }
                }
            }
        } else {
            String[] attributeNames = inputDefinition.getAttributeNameArray();
            for (String attributeName : attributeNames) {
                if (featureIndexMap.get(attributeName) != null) {
                    int featureIndex = featureIndexMap.get(attributeName);
                    int newFeatureIndex = newToOldIndicesList.indexOf(featureIndex);
                    int[] attributeIndexArray = new int[4];
                    attributeIndexArray[2] = 2; // get values from output data
                    attributeIndexArray[3] = inputDefinition.getAttributePosition(attributeName);
                    attributeIndexMap.put(newFeatureIndex, attributeIndexArray);
                } else {
                    throw new SiddhiAppCreationException(
                            "No matching feature name found in the models for the attribute : "
                                    + attributeName);
                }
            }
        }
    }

    /**
     * Return the Attribute.Type defined by the data-type argument.
     * 
     * @param dataType data type of the output attribute
     * @return Attribute.Type object corresponding to the dataType
     */
    private Attribute.Type getOutputAttributeType(String dataType) {

        if (dataType.equalsIgnoreCase("double")) {
            return Attribute.Type.DOUBLE;
        } else if (dataType.equalsIgnoreCase("float")) {
            return Attribute.Type.FLOAT;
        } else if (dataType.equalsIgnoreCase("integer") || dataType.equalsIgnoreCase("int")) {
            return Attribute.Type.INT;
        } else if (dataType.equalsIgnoreCase("long")) {
            return Attribute.Type.LONG;
        } else if (dataType.equalsIgnoreCase("string")) {
            return Attribute.Type.STRING;
        } else if (dataType.equalsIgnoreCase("boolean") || dataType.equalsIgnoreCase("bool")) {
            return Attribute.Type.BOOL;
        } else {
            throw new SiddhiAppValidationException("Invalid data-type defined for response " +
                    "variable.");
        }
    }

    private void logError(int modelId, Exception e) {
        log.error("Error while retrieving ML-model : " + modelStorageLocations[modelId], e);
        throw new SiddhiAppCreationException(
                "Error while retrieving ML-model : " + modelStorageLocations[modelId] + "\n"
                        + e.getMessage());
    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        return new HashMap<>();
    }

    @Override
    public void restoreState(Map<String, Object> var1) {

    }
}
