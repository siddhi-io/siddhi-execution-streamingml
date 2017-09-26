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

package org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.DataPoint;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.KMeansClusterer;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.KMeansModel;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * performs kmeans with batch update
 */
@Extension(
        name = "kMeansMiniBatch",
        namespace = "streamingml",
        description = "Performs K-Means clustering on a streaming data set. Data points can be of " +
                "any dimension and the dimensionality is calculated from number of parameters. " +
                "All data points to be processed in a single query should be of the" +
                " same dimensionality. The Euclidean distance is taken as the distance metric. " +
                "The algorithm resembles mini-batch K-Means. (refer Web-Scale K-Means Clustering by " +
                "D.Sculley, Google, Inc.). ",
        parameters = {
                @Parameter(
                        name = "no.of.clusters",
                        description = "The assumed number of natural clusters in the data set.",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "decay.rate",
                        description = "this is the decay rate of old data compared to new data. " +
                                "Value of this will be in [0,1]. 0 means only old data used and" +
                                "1 will mean that only new data is used",
                        optional = true,
                        type = {DataType.DOUBLE},
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "maximum.iterations",
                        description = "Number of iterations, the process iterates until the number of maximum " +
                                "iterations is reached or the centroids do not change",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "50"
                ),
                @Parameter(
                        name = "no.of.events.to.retrain",
                        description = "number of events to recalculate cluster centers. ",
                        type = DataType.INT,
                        optional = true,
                        defaultValue = "20"
                ),
                @Parameter(
                        name = "model.features",
                        description = "This is a variable length argument. Depending on the dimensionality of " +
                                "data points we will receive coordinates as features along each axis.",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG}
                )

        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "euclideanDistanceToClosestCentroid",
                        description = "Represents the Euclidean distance between the current data point and the " +
                                "closest centroid.",
                        type = {DataType.DOUBLE}
                ),
                @ReturnAttribute(
                        name = "closestCentroidCoordinate",
                        description = "This is a variable length attribute. Depending on the dimensionality(d) " +
                                "we will return closestCentroidCoordinate1 to closestCentroidCoordinated which are " +
                                "the d dimensional coordinates of the closest centroid from the model to the " +
                                "current event. This is the prediction result and this represents the cluster to" +
                                "which the current event belongs to.",
                        type = {DataType.DOUBLE}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (x double, y double);\n" +
                                "@info(name = 'query1')\n" +
                                "from InputStream#streamingml:kMeansMiniBatch(2, 0.2, 10, 20, x, y)\n" +
                                "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y\n" +
                                "insert into OutputStream;",
                        description = "This is an example where user gives all three hyper parameters. first 20 " +
                                "events will be consumed to build the model and from the 21st event prediction " +
                                "would start"
                ),
                @Example(
                        syntax = "define stream InputStream (x double, y double);\n" +
                                "@info(name = 'query1')\n" +
                                "from InputStream#streamingml:kMeansMiniBatch(2, x, y)\n" +
                                "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y\n" +
                                "insert into OutputStream;",
                        description = "This is an example where user has not specified hyper params. So default " +
                                "values will be used."
                )
        }
)
public class KMeansMiniBatchSPExtension extends StreamProcessor {
    private double decayRate = 0.01;
    private int numberOfEventsToRetrain = 20;
    private int numberOfEventsReceived;
    private LinkedList<DataPoint> dataPointsArray;
    private double[] coordinateValuesOfCurrentDataPoint;
    private int maximumIterations = 50;
    private int numberOfClusters;
    private KMeansModel kMeansModel;
    private int dimensionality;
    private ExecutorService executorService;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new LinkedList<>();
    private static final Logger logger = Logger.getLogger(KMeansMiniBatchSPExtension.class.getName());

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        final int minConstantParams = 1;
        final int maxConstantParams = 4;
        final int minNumberOfFeatures = 1;
        int coordinateStartIndex;
        int maxNoOfFeatures = inputDefinition.getAttributeList().size();
        dataPointsArray = new LinkedList<>();

        if (attributeExpressionLength < minConstantParams + minNumberOfFeatures ||
                attributeExpressionLength > maxConstantParams + maxNoOfFeatures) {
            throw new SiddhiAppCreationException("Invalid number of parameters. User can either choose to give " +
                    "all 3 hyper parameters or none at all.");
        }

        //expressionExecutors[0] --> numberOfClusters
        if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppCreationException("1st query parameter is numberOfClusters which has to be constant" +
                    "but found " + this.attributeExpressionExecutors[0].getClass().getCanonicalName());
        }
        if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
            numberOfClusters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
        } else {
            throw new SiddhiAppCreationException("The first query parameter should numberOfClusters which should " +
                    "be of type int but found " + attributeExpressionExecutors[0].getReturnType());
        }

        if (attributeExpressionExecutors[1] instanceof VariableExpressionExecutor) {
            coordinateStartIndex = 1;

        } else {
            coordinateStartIndex = 4;
            //expressionExecutors[1] --> decayRate
            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("Decay rate has to be a constant but found " +
                        this.attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
                decayRate = (double) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue();
                if (decayRate < 0 || decayRate > 1) {
                    throw new SiddhiAppCreationException("Decay rate should be in [0,1] but given as " + decayRate);
                }
            } else {
                throw new SiddhiAppCreationException("Decay rate should be of type int but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            //expressionExecutors[2] --> maximumIterations
            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("Maximum iterations has to be a constant but found " +
                        this.attributeExpressionExecutors[2].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                maximumIterations = (Integer) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[2]).getValue();
                if (maximumIterations <= 0) {
                    throw new SiddhiAppCreationException("maxIterations should be a positive integer " +
                            "but found " + maximumIterations);
                }
            } else {
                throw new SiddhiAppCreationException("Maximum iterations should be of type int but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

            //expressionExecutors[3] --> numberOfEventsToRetrain
            if (!(attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("numberOfEventsToRetrain has to be a constant but found " +
                        this.attributeExpressionExecutors[3].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                numberOfEventsToRetrain = (Integer) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[3]).getValue();
                if (numberOfEventsToRetrain <= 0) {
                    throw new SiddhiAppCreationException("numberOfEventsToRetrain should be a positive integer " +
                            "but found " + numberOfEventsToRetrain);
                }
            } else {
                throw new SiddhiAppCreationException("numberOfEventsToRetrain should be of type int but found " +
                        attributeExpressionExecutors[3].getReturnType());
            }
        }

        dimensionality = attributeExpressionLength - coordinateStartIndex;
        coordinateValuesOfCurrentDataPoint = new double[dimensionality];

        //validating all the features
        featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                attributeExpressionExecutors, coordinateStartIndex, dimensionality);

        kMeansModel = new KMeansModel();

        executorService = siddhiAppContext.getExecutorService();

        List<Attribute> attributeList = new ArrayList<>(1 + dimensionality);
        attributeList.add(new Attribute("euclideanDistanceToClosestCentroid", Attribute.Type.DOUBLE));
        for (int i = 1; i <= dimensionality; i++) {
            attributeList.add(new Attribute("closestCentroidCoordinate" + i, Attribute.Type.DOUBLE));
        }
        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                numberOfEventsReceived++;

                //validating and getting coordinate values
                for (int i = 0; i < dimensionality; i++) {
                    try {
                        Number content = (Number) featureVariableExpressionExecutors.get(i).execute(streamEvent);
                        coordinateValuesOfCurrentDataPoint[i] = content.doubleValue();
                    } catch (ClassCastException e) {
                        throw new SiddhiAppRuntimeException("coordinate values should be int/float/double/long " +
                                "but found " +
                                attributeExpressionExecutors[i].execute(streamEvent).getClass());
                    }
                }

                //creating a dataPoint with the received coordinate values
                DataPoint currentDataPoint = new DataPoint();
                currentDataPoint.setCoordinates(coordinateValuesOfCurrentDataPoint);
                dataPointsArray.add(currentDataPoint);

                if (kMeansModel.isTrained()) {
                    logger.debug("Populating output");
                    complexEventPopulater.populateComplexEvent(streamEvent,
                            KMeansClusterer.getAssociatedCentroidInfo(currentDataPoint, kMeansModel));
                }

                //handling the training
                if (numberOfEventsReceived % numberOfEventsToRetrain == 0) {
                    KMeansClusterer.train(new LinkedList<>(dataPointsArray), numberOfEventsToRetrain, decayRate,
                            executorService, kMeansModel, numberOfClusters, maximumIterations, dimensionality);
                    dataPointsArray.clear();
                }

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
        synchronized (this) {
            Map<String, Object> map = new HashMap();
            map.put("untrainedData", dataPointsArray);
            map.put("numberOfEventsReceived", numberOfEventsReceived);
            map.put("kMeansModel", kMeansModel);
            logger.debug("storing kmeans model " + map.get("kMeansModel"));
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            dataPointsArray = (LinkedList<DataPoint>) map.get("untrainedData");
            numberOfEventsReceived = (Integer) map.get("numberOfEventsReceived");
            kMeansModel = (KMeansModel) map.get("kMeansModel");
        }
    }
}
