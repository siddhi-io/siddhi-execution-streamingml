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
import org.wso2.extension.siddhi.execution.streamingml.classification.hoeffdingtree.util.PrequentialModelEvaluation;
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
import java.util.concurrent.ExecutorService;

/**
 * Build/update using a Hoeffding Adaptive Tree Model.
 * {@link HoeffdingClassifierUpdaterStreamProcessorExtension}
 */
@Extension(
        name = "hoeffdingLearn",
        namespace = "streamingml",
        description = "Performs classification with Hoeffiding Adaptive Tree monitoring " +
                "Concept drift with ADWIN ",
        parameters = {
                @Parameter(name = "model.name",
                        description = "The name of the model to be build/updated.",
                        type = {DataType.STRING}),
                @Parameter(name = "number.ofclasses",
                        description = "The number of Class labels in the datastream.",
                        type = {DataType.INT}),
                @Parameter(name = "grace.period",
                        description = "The number of instances a leaf should observe between split attempts. " +
                                "default:200, min:0, max:2147483647",
                        type = {DataType.INT}),
                @Parameter(name = "split.criterion",
                        description = "Split criterion to use. 0:InfoGainSplitCriterion, 1:GiniSplitCriterion",
                        type = {DataType.INT}),
                @Parameter(name = "split.confidence",
                        description = "The allowable error in split decision, values closer to 0 will take " +
                                "longer to decide. default:1e-7",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "breaktie.threshold",
                        description = "Threshold below which a split will be forced to break ties. " +
                                "default:0.05D, min:0.0D, max:1.0D",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "binarysplit.flag",
                        description = "Only allow binary splits. default:false",
                        type = {DataType.BOOL}),
                @Parameter(name = "disable.prepruning.flag",
                        description = "Disable pre-pruning",
                        type = {DataType.BOOL}),
                @Parameter(name = "leafprediction.strategy",
                        description = "Leaf prediction to use. " +
                                "0:Majority class, 1:Naive Bayes, 2:Naive Bayes Adaptive. default:2",
                        type = {DataType.INT}),
                @Parameter(name = "model.features",
                        description = "Features of the model which should be attributes of the stream.",
                        type = {DataType.DOUBLE, DataType.INT})
        },
        //todo:
        returnAttributes = {
                @ReturnAttribute(name = "prediction",
                        description = "The predicted value (`true/false`)",
                        type = {DataType.BOOL}),
                @ReturnAttribute(name = "prequentialEvaluation",
                        description = "The probability of the prediction",
                        type = {DataType.DOUBLE})
        },
        //todo: do examples
        examples = {
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, " +
                                "attribute_2 double, attribute_3 double, attribute_4 string );\n" +
                                "\n" +
                                "from StreamA#ml:hoeffdingLearn('model1', 3) \n" +
                                "select attribute_0, attribute_1, attribute_2, attribute_3, " +
                                "accuracy insert into outputStream;",
                        description = "A HoeffdingTree model with the name 'model1' will be built/updated under " +
                                "3 number of classes using attribute_0, attribute_1, attribute_2, attribute_3 " +
                                "as features and attribute_4 as the label."
                ),
                @Example(
                        syntax = "define stream StreamA (attribute_0 double, attribute_1 double, " +
                                "attribute_2 double, attribute_3 double, attribute_4 string );\n" +
                                "\n" +
                                "from StreamA#ml:hoeffdingLearn('model1', 3, 200, 0, 1e-7, 1.0D, true, true, 2) \n" +
                                "select attribute_0, attribute_1, attribute_2, attribute_3, " +
                                "accuracy insert into outputStream;",
                        description = "A Hoeffding Tree model with the name 'model1' will be " +
                                "built/updated with a grace period of 200, InformationGain Split criterion," +
                                "1e-7 of allowable error in split decision, 1.0D of breaktie threshold, " +
                                "allowing only binary splits, disabled pre-pruning, Naive Bayes Adaptive as" +
                                "leaf prediction using attribute_0, attribute_1, attribute_2, attribute_3 as " +
                                "features and attribute_4 as the label. Updated weights of the " +
                                "model will be emitted to the outputStream."
                )
        }
)
public class HoeffdingClassifierUpdaterStreamProcessorExtension extends StreamProcessor {

    private static final Logger logger = Logger.getLogger(HoeffdingClassifierUpdaterStreamProcessorExtension.class);
    private ExecutorService executorService;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();
    private VariableExpressionExecutor classLabelVariableExecutor;

    private int numberOfAttributes;
    private int numberOfClasses;
    private int parameterPosition = 2;
    private String modelName;

    // values of class attribute
    private List<String> classes = new ArrayList<String>();
    private PrequentialModelEvaluation evolutionModel;

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        executorService = siddhiAppContext.getExecutorService();
        String siddhiAppName = siddhiAppContext.getName();
        String modelPrefix;
        int classIndex = attributeExpressionLength - 1;
        int maxNumberOfFeatures = inputDefinition.getAttributeList().size();

        if (maxNumberOfFeatures + parameterPosition >= 5) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    modelPrefix = (String) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[0])
                            .getValue();
                    // model name = user given name + siddhi app name
                    modelName = modelPrefix + "." + siddhiAppName;
                } else {
                    throw new SiddhiAppValidationException(
                            "Invalid parameter type found for the model.name argument, " + "required "
                                    + Attribute.Type.STRING
                                    + " but found " + attributeExpressionExecutors[0].
                                    getReturnType().toString());
                }
            } else {
                throw new SiddhiAppValidationException(
                        "Model.name must be"
                                + " (ConstantExpressionExecutor) but found "
                                + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }

            //2nd parameter
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    numberOfClasses = (Integer) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[1])
                            .getValue();
                    if (numberOfClasses < 2) {
                        throw new SiddhiAppValidationException(
                                "Number of classes must be greater than 1 but" + " found "
                                        + numberOfClasses);
                    }
                } else {
                    throw new SiddhiAppValidationException(
                            "Invalid parameter type found for the numberOfClasses argument, " + "required "
                                    + Attribute.Type.INT
                                    + " but found " + attributeExpressionExecutors[1].
                                    getReturnType().toString());
                }
            } else {
                throw new SiddhiAppValidationException(
                        "Number of classes count must be"
                                + " (ConstantExpressionExecutor) but found "
                                + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            //3rd parameter
            if (attributeExpressionExecutors[parameterPosition] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[parameterPosition].getReturnType() == Attribute.Type.INT) {
                    //configuring hoeffding tree model with hyper-parameters
                    logger.debug("Hoeffding Adaptive Tree is configured with hyper-parameters");
                    configureModelWithHyperParameters(modelName);
                }
            } else {
               /* numberOfAttributes = attributeExpressionExecutors.length - parameterPosition;*/
                numberOfAttributes = maxNumberOfFeatures;
            }
            if (numberOfAttributes < 2) {
                throw new SiddhiAppValidationException(
                        "Number of attributes must be greater than 1 but" + " found "
                                + numberOfAttributes);
            }
            featureVariableExpressionExecutors = CoreUtils
                    .extractAndValidateFeatures(inputDefinition, attributeExpressionExecutors,
                            parameterPosition, classIndex);

            classLabelVariableExecutor = CoreUtils
                    .extractAndValidateClassLabel(inputDefinition, attributeExpressionExecutors,
                            classIndex);

            getModel(modelName);
            evolutionModel = new PrequentialModelEvaluation();
            evolutionModel.reset(numberOfClasses);

        } else {
            throw new SiddhiAppValidationException(String.format("Invalid number of parameters for " +
                    "ml:hoeffdingLearn. This Stream Processor requires at least 2 parameters, namely, " +
                    "model.name, number_of_classes, but found %s parameters", attributeExpressionExecutors.length));
        }

        //set attributes for OutputStream
        List<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(new Attribute("prediction", Attribute.Type.STRING));
        return attributes;
    }


    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor
            processor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[numberOfAttributes];

                    String classValue = classLabelVariableExecutor.execute(complexEvent).toString();

                    // Set class value
                    if (classValue.equals("?")) { // these data points for prediction
                        cepEvent[numberOfAttributes - 1] = -1;
                        throw new SiddhiAppRuntimeException(
                                "The class value is not accepted. Found ? for class value");

                    } else {
                        if (classes.contains(classValue)) {
                            cepEvent[numberOfAttributes - 1] = classes.indexOf(classValue);
                        } else {
                            if (classes.size() < numberOfClasses) {
                                classes.add(classValue);
                                cepEvent[numberOfAttributes - 1] = classes.indexOf(classValue);
                            } else {
                                throw new SiddhiAppRuntimeException(
                                        "Number of classes " + numberOfClasses + " but found "
                                                + classes.size());
                            }
                        }
                    }

                    for (int i = 0; i < numberOfAttributes - 1; i++) {
                        cepEvent[i] = ((Number) featureVariableExpressionExecutors.get(i)
                                .execute(complexEvent)).doubleValue();
                    }

                    AdaptiveHoeffdingTreeModel model = AdaptiveHoeffdingModelsHolder.getInstance()
                            .getHoeffdingModel(modelName);


                    /*model.trainOnEvent(cepEvent, classes);*/
                    model.testThenTrainOnEvent(evolutionModel, cepEvent, classes);

                    complexEventPopulater.populateComplexEvent(complexEvent, new Object[]{"Trained Successfully"});
                }
                nextProcessor.process(streamEventChunk);
            }
        }
    }


    private AdaptiveHoeffdingTreeModel getModel(String modelName) {
        AdaptiveHoeffdingTreeModel model = AdaptiveHoeffdingModelsHolder.getInstance().getHoeffdingModel(modelName);
        if (model == null) {
            logger.debug("Creating new model Hoeffding Adaptive Tree model with the name, " + modelName);
            model = new AdaptiveHoeffdingTreeModel();
            AdaptiveHoeffdingModelsHolder.getInstance().addHoeffdingModel(modelName, model);
        }
        if (model.getStreamHeader() != null) {
            // validate the model
            if (numberOfAttributes != model.getNumberOfAttributes()) {
                // clean the model
                logger.debug("Deleting the model " + modelName);
                AdaptiveHoeffdingModelsHolder.getInstance().deleteHoeffdingModel(modelName);
                throw new SiddhiAppValidationException(String.format("Model [%s] expects %s " +
                                "features, but the streamingml:hoeffdingClassifier " +
                                "specifies %s features", modelName, model.getStreamHeader().numAttributes()
                        , numberOfAttributes));
            }
        } else {
            model.init(numberOfAttributes);
        }
        return model;
    }


    private void configureModelWithHyperParameters(String modelName) {
        int numberOfHyperParameters = 7;

        //default configurations for Hoeffding Adaptive tree
        int gracePeriod = 200;
        int splittingCriteria = 1;
        double allowableSplitError = 1e-7;
        double breakTieThreshold = 0.05;
        boolean binarySplit = false;
        boolean disablePrePruning = false;
        int leafpredictionStrategy = 2;

        while (attributeExpressionExecutors[parameterPosition] instanceof ConstantExpressionExecutor) {
            switch (parameterPosition) {
                case 3:
                    if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                        gracePeriod = (Integer) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[3])
                                .getValue();
                    } else {
                        throw new SiddhiAppValidationException("GracePeriod must be an Integer." +
                                " But found " + attributeExpressionExecutors[3].getClass().getCanonicalName());
                    }
                    break;
                case 4:
                    if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.INT) {
                        splittingCriteria = (Integer) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[4])
                                .getValue();
                    } else {
                        throw new SiddhiAppValidationException("Splitting Criteria must be an Integer. " +
                                "0=InfoGainSplitCriterion and 1=GiniSplitCriterion" +
                                " But found " + attributeExpressionExecutors[4].getClass().getCanonicalName());
                    }
                    break;
                case 5:
                    if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.DOUBLE) {
                        allowableSplitError = (double) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[5])
                                .getValue();
                    } else {
                        throw new SiddhiAppValidationException("AllowableSplitError must be a Double." +
                                " But found " + attributeExpressionExecutors[5].getClass().getCanonicalName());
                    }
                    break;
                case 6:
                    if (attributeExpressionExecutors[6].getReturnType() == Attribute.Type.DOUBLE) {
                        breakTieThreshold = (double) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[6])
                                .getValue();
                    } else {
                        throw new SiddhiAppValidationException("BreakTieThreshold must be a Double." +
                                " But found " + attributeExpressionExecutors[6].getClass().getCanonicalName());
                    }
                    break;
                case 7:
                    if (attributeExpressionExecutors[7].getReturnType() == Attribute.Type.BOOL) {
                        binarySplit = (boolean) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[7])
                                .getValue();
                    } else {
                        throw new SiddhiAppValidationException("BinarySplitOption must be an Integer." +
                                " But found " + attributeExpressionExecutors[7].getClass().getCanonicalName());
                    }
                    break;
                case 8:
                    if (attributeExpressionExecutors[8].getReturnType() == Attribute.Type.BOOL) {
                        disablePrePruning = (boolean) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[8])
                                .getValue();
                    } else {
                        throw new SiddhiAppValidationException("DisablePrePruning must be a Boolean." +
                                " But found " + attributeExpressionExecutors[8].getClass().getCanonicalName());
                    }
                    break;
                case 9:
                    if (attributeExpressionExecutors[9].getReturnType() == Attribute.Type.INT) {
                        leafpredictionStrategy = (int) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[9])
                                .getValue();
                    } else {
                        throw new SiddhiAppValidationException("LeafPredictionStrategy must be an Integer. " +
                                "0=majority class, 1=naive Bayes, 2=naive Bayes adaptive" +
                                " But found " + attributeExpressionExecutors[9].getClass().getCanonicalName());
                    }
                    break;
            }
            parameterPosition++;
        }
        if (parameterPosition == 10) {
            numberOfAttributes = attributeExpressionExecutors.length - parameterPosition;
            AdaptiveHoeffdingTreeModel model = getModel(modelName);
            model.setConfigurations(gracePeriod, splittingCriteria, allowableSplitError,
                    breakTieThreshold, binarySplit, disablePrePruning, leafpredictionStrategy);

        } else {
            throw new SiddhiAppValidationException("Number of hyper-parameters needed for model " +
                    "manual configuration is " + numberOfHyperParameters + " but found " + (parameterPosition - 2));
        }
    }


    @Override
    public void start() {
    }

    @Override
    public void stop() {
        //delete model
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
