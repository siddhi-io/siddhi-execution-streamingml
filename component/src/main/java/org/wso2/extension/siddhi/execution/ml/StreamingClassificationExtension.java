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

package org.wso2.extension.siddhi.execution.ml;

import org.wso2.extension.siddhi.execution.ml.samoa.utils.classification.
        StreamingClassification;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class StreamingClassificationExtension extends StreamProcessor
        implements SchedulingProcessor {

    private int numberOfAttributes;
    private int numberOfClasses;
    private int numberOfNominals;
    private int parameterPosition;
    private int numberOfNumerics;
    private Scheduler scheduler;
    private long lastScheduledTimestamp = -1;
    private long TIMER_DURATION = 100;
    private ExecutorService executorService;

    private StreamingClassification streamingClassification;

    private List<String> classes = new ArrayList<String>();            //values of class attribute
    private List<ArrayList<String>> nominals = new ArrayList<ArrayList<String>>();//values of other nominal attributes

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        executorService = executionPlanContext.getExecutorService();
        int maxEvents = -1;
        int interval = 1000;
        int parallelism = 1;
        int numberModelsBagging = 0;
        String nominalAttributesValues;

        if (attributeExpressionExecutors.length >= 5) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                    numberOfAttributes = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                            .getValue();
                    if (numberOfAttributes < 2) {
                        throw new ExecutionPlanValidationException("Number of attributes must be greater than 1 but" +
                                " found " + numberOfAttributes);
                    }
                    if (numberOfAttributes + 1 > attributeExpressionExecutors.length) {
                        throw new ExecutionPlanValidationException("There is a inconsistency with numbxer of " +
                                "attributes and entered attributes. Number of attributes should be greater than " +
                                numberOfAttributes + " or entered attributes should be remove.");
                    }
                    for (int i = attributeExpressionExecutors.length - numberOfAttributes; i <
                            attributeExpressionExecutors.length; i++) {
                        if (!(attributeExpressionExecutors[i] instanceof VariableExpressionExecutor)) {
                            throw new ExecutionPlanValidationException("Parameter number " + (i + 1) + " is not an " +
                                    "attribute. Check the number of attribute entered as a attribute set with number " +
                                    "of attribute configuration parameter");
                        }
                    }
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for the first argument, " +
                            "required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[0].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Parameter count must be a constant (" +
                        ConstantExpressionExecutor.class.getCanonicalName() + ") but found " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }

            parameterPosition = attributeExpressionExecutors.length - numberOfAttributes;

            if (parameterPosition > 2) {
                if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                        numberOfClasses = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                                .getValue();
                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for the second " +
                                "argument, required " + Attribute.Type.INT + " but found " +
                                attributeExpressionExecutors[1].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("Number of classes count must be" +
                            " a constant (" + ConstantExpressionExecutor.class.getCanonicalName() + ") but found " +
                            attributeExpressionExecutors[1].getClass().getCanonicalName());
                }

                if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.STRING) {
                        nominalAttributesValues = (String) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[2]).getValue();
                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for the third " +
                                "argument, required " + Attribute.Type.STRING + " but found "
                                + attributeExpressionExecutors[2].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("Nominal attributes values must be a constant (" +
                            ConstantExpressionExecutor.class.getCanonicalName() + ") but found " +
                            attributeExpressionExecutors[2].getClass().getCanonicalName() + " value.");
                }
                if (nominalAttributesValues.isEmpty() || nominalAttributesValues.equals(" ")) {
                    numberOfNominals = 0;
                } else {
                    String[] temp = nominalAttributesValues.split(",");
                    numberOfNominals = temp.length;
                }
            } else {
                throw new ExecutionPlanValidationException("Invalid number of parameters. " + parameterPosition +
                        " parameters found, but required at least 3 configuration parameters. " +
                        " streamingClassificationSamoa(parameterCount, numberOfClasses, valuesOfNominals, " +
                        "attribute_set) ");
            }
            if (parameterPosition > 3) {
                if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {

                        interval = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue();
                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                                " the fourth argument, required " + Attribute.Type.INT + " but found " +
                                attributeExpressionExecutors[3].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("Interval size values must be a constant " +
                            "(" + ConstantExpressionExecutor.class.getCanonicalName() + ") but found " +
                            "(" + attributeExpressionExecutors[3].getClass().getCanonicalName() + ") value.");
                }
            }

            if (parameterPosition > 4) {
                if (attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.INT) {
                        parallelism = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[4])
                                .getValue();
                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                                " the fifth argument, required " + Attribute.Type.INT + " but found "
                                + attributeExpressionExecutors[4].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("Parallelism value must be a constant ("
                            + ConstantExpressionExecutor.class.getCanonicalName() + ") but found (" +
                            attributeExpressionExecutors[4].getClass().getCanonicalName() + ") value.");
                }
            }

            if (parameterPosition > 5) {
                if (attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.INT) {
                        numberModelsBagging = (Integer) ((ConstantExpressionExecutor)
                                attributeExpressionExecutors[5]).getValue();
                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for " +
                                "the sixth argument, required " + Attribute.Type.INT + " but found " +
                                attributeExpressionExecutors[5].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("Number of models(For bagging) must be a constant (" +
                            ConstantExpressionExecutor.class.getCanonicalName() + ") but found (" +
                            attributeExpressionExecutors[5].getClass().getCanonicalName() + ") value.");
                }
            }

            if (parameterPosition > 6) {
                if (attributeExpressionExecutors[6] instanceof ConstantExpressionExecutor) {
                    if (attributeExpressionExecutors[6].getReturnType() == Attribute.Type.INT) {
                        maxEvents = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[6]).getValue();
                        if (maxEvents < -1) {
                            throw new ExecutionPlanValidationException("Maximum number of events must be greater than" +
                                    " or equal -1. (-1 = No limit), but found " + maxEvents);
                        }

                    } else {
                        throw new ExecutionPlanValidationException("Invalid parameter type found for the seventh " +
                                "argument, required " + Attribute.Type.INT + " but found " +
                                attributeExpressionExecutors[6].getReturnType().toString());
                    }
                } else {
                    throw new ExecutionPlanValidationException("Maximum events count must be a constant (" +
                            ConstantExpressionExecutor.class.getCanonicalName() + ") but found (" +
                            attributeExpressionExecutors[6].getClass().getCanonicalName() + ") value.");
                }
            }
        } else {
            throw new ExecutionPlanValidationException("Parameter count should be greater than 4  " +
                    "but found " + attributeExpressionExecutors.length);
        }
        numberOfNumerics = numberOfAttributes - numberOfNominals - 1;
        lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime();
        streamingClassification = new StreamingClassification(maxEvents, interval, numberOfClasses,
                numberOfAttributes, numberOfNominals, nominalAttributesValues,
                parallelism, numberModelsBagging);

        List<Attribute> attributes = new ArrayList<Attribute>(numberOfAttributes);
        for (int i = 0; i < numberOfAttributes - 1; i++) {
            if (i < numberOfNumerics) {
                attributes.add(new Attribute("att_" + i, Attribute.Type.DOUBLE));
            } else {
                attributes.add(new Attribute("att_" + i, Attribute.Type.STRING));
            }
        }
        attributes.add(new Attribute("prediction", Attribute.Type.STRING));
        return attributes;
    }

    /**
     * Process events received by StreamingClassificationExtension
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater
                                   complexEventPopulater) {

//        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[attributeExpressionLength - parameterPosition];
                    // Set cep event values
                    String classValue = (String) attributeExpressionExecutors[attributeExpressionLength - 1].
                            execute(complexEvent);
                    //Set class value
                    if (classValue.equals("?")) {           //these data points for prediction
                        cepEvent[numberOfAttributes - 1] = -1;
                    } else {
                        // These data points have class values, therefore these data use to train and test the model
                        if (classes.contains(classValue)) {
                            cepEvent[numberOfAttributes - 1] = classes.indexOf(classValue);
                        } else {
                            if (classes.size() < numberOfClasses) {
                                classes.add(classValue);
                                cepEvent[numberOfAttributes - 1] = classes.indexOf(classValue);
                            } else {
                                throw new ExecutionPlanRuntimeException("Number of classes " +
                                        numberOfClasses + " but found " + classes.size());
                            }
                        }
                    }
                    int nominalIndex = 0;

                    // Set other attributes
                    for (int i = 0; i < numberOfAttributes - 1; i++) {
                        Object eventValue = attributeExpressionExecutors[i + parameterPosition].
                                execute(complexEvent);

                        if (i < numberOfNumerics) {         // set Numerical attributes
                            cepEvent[i] = ((Number) eventValue).doubleValue();
                        } else {
                            // Set nominal attributes
                            // TODO: 12/22/16 need to clear the nominals values in the long term
                            setNominalValues(nominalIndex, eventValue);
                            cepEvent[i] = nominals.get(nominalIndex).indexOf(eventValue.toString());
                            nominalIndex++;
                        }
                    }
                    streamingClassification.addEvents(cepEvent);

                    Object[] outputData = streamingClassification.getOutput();
                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {       // If output has values, then add those values to output stream
                        int indexPredict = (int) outputData[outputData.length - 1];
                        outputData[outputData.length - 1] = classes.get(indexPredict);
                        if (numberOfNominals > 0) {
                            outputData = outputNominals(outputData);
                        }
//                        StreamEvent streamEvent1 = new StreamEvent(0, 0, outputData.length);
//                        streamEvent1.setOutputData(outputData);
//                        complexEventChunk.add(streamEvent1);

                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }
                }
                // TODO: 12/22/16 try to remove it  
//                else { // Timer events for poll output events from prediction queue
//                    lastScheduledTimestamp = lastScheduledTimestamp + TIMER_DURATION;
//                    scheduler.notifyAt(lastScheduledTimestamp);
//                    Object[] outputData = null;
//                    outputData = streamingClassification.getOutput();
//                    if (outputData == null) {
//                        streamEventChunk.remove();
//                    } else {
//                        int index_predict = (int) outputData[outputData.length - 1];
//                        outputData[outputData.length - 1] = classes.get(index_predict);
//                        if (numberOfNominals != 0) {
//                            outputData = outputNominals(outputData);
//                        }
////                        StreamEvent streamEvent1 = new StreamEvent(0, 0, outputData.length);
////                        streamEvent1.setOutputData(outputData);
////                        complexEventChunk.add(streamEvent1);
//                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
//
//                    }
//
//                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    /**
     * Set nominal values to nominal value array
     *
     * @param nominalIndex
     * @param eventValue
     */
    private void setNominalValues(int nominalIndex, Object eventValue) {
        if (nominals.size() > nominalIndex) {
            if (!nominals.get(nominalIndex).contains(eventValue)) {
                nominals.get(nominalIndex).add(eventValue.toString());
            }
        } else {
            nominals.add(new ArrayList<String>());
            nominals.get(nominalIndex).add(eventValue.toString());
        }
    }

    /**
     * Convert double output values to its original form (String)
     *
     * @param output Output from samoa
     * @return converted output attributes
     */
    private Object[] outputNominals(Object[] output) {
        Object[] outputData = output;
        for (int k = numberOfNumerics; k < numberOfAttributes - 1; k++) {
            int nominal_index = ((Double) outputData[k]).intValue();
            outputData[k] = nominals.get(k - numberOfNumerics).
                    get(nominal_index);
        }
        return outputData;
    }

    @Override
    public void start() {
        executorService.execute(streamingClassification);

    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[]{scheduler, lastScheduledTimestamp, TIMER_DURATION,
                streamingClassification};
    }

    @Override
    public void restoreState(Object[] state) {
        scheduler = (Scheduler) state[0];
        lastScheduledTimestamp = (long) state[1];
        TIMER_DURATION = (long) state[2];
        streamingClassification = (StreamingClassification) state[3];
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (lastScheduledTimestamp > 0) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() +
                    TIMER_DURATION;
            scheduler.notifyAt(lastScheduledTimestamp);
        }
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }
}
