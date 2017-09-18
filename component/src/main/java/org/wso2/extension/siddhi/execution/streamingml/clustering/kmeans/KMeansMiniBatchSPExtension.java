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
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.Clusterer;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.DataPoint;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.KMeansModel;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.KMeansModelHolder;
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
                        name = "model.name",
                        description = "The name for the model that is going to be created/reused for prediction",
                        type = {DataType.STRING}
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
                        name = "no.of.clusters",
                        description = "The assumed number of natural clusters (numberOfClusters) in the data set.",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "max.iterations",
                        description = "Number of iterations, the process iterates until the number of maximum " +
                                "iterations is reached or the centroids do not change",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "no.of.events.to.retrain",
                        description = "number of events to recalculate cluster centers. ",
                        type = DataType.INT
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
                        syntax = "define stream InputStream (modelFeature1 double, modelFeature2 double);" +
                                "from InputStream#streamingml:kmeansminibatch(modelName, numberOfClusters, " +
                                "maxIterations," +
                                " numberOfEventsToRetrain, decayRate, modelFeature1, modelFeature2)\"\n" +
                                "select modelFeature1, modelFeature2, euclideanDistanceToClosestCentroid, " +
                                "closestCentroidCoordinate1, closestCentroidCoordinate2\"\n" +
                                "insert into OutputStream",
                        description = "modelName ='model1', numberOfClusters=2, numberOfEventsToRetrain = 5, " +
                                "maxIterations=10" +
                                " decayRate=0.2. This will cluster the collected data points within the window " +
                                "for every 5 events" +
                                "and give output after the first 5 events. Retraining will also happen after " +
                                "every 5 events"
                ),
        }
)
public class KMeansMiniBatchSPExtension extends StreamProcessor {
    private double decayRate;
    private int numberOfEventsToRetrain;
    private int numberOfEventsReceived;
    private LinkedList<DataPoint> dataPointsArray;
    private double[] coordinateValuesOfCurrentDataPoint;
    private boolean isModelInitialTrained;
    private Clusterer clusterer;
    private int dimensionality;
    private String modelName;
    private ExecutorService executorService;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new LinkedList<>();
    private static final Logger logger = Logger.getLogger(KMeansMiniBatchSPExtension.class.getName());

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        dataPointsArray = new LinkedList<>();
        int numberOfClusters;

        //expressionExecutors[0] --> modelName
        if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppCreationException("modelName has to be a constant but found " +
                    this.attributeExpressionExecutors[0].getClass().getCanonicalName());
        }

        if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
            modelName = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
        } else {
            throw new SiddhiAppCreationException("modelName should be of type String but found " +
                    attributeExpressionExecutors[0].getReturnType());
        }

        //expressionExecutors[1] --> decayRate or numberOfClusters
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppCreationException("2nd parameter can be decayRate/numberOfClusters. " +
                    "Both has to be a constant but found " +
                    this.attributeExpressionExecutors[1].getClass().getCanonicalName());
        }
        int coordinateStartIndex;
        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
            if (logger.isDebugEnabled()) {
                logger.debug("Decay rate is specified." + siddhiAppContext.getName());
            }
            decayRate = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            if (decayRate < 0 || decayRate > 1) {
                throw new SiddhiAppCreationException("decayRate should be in [0,1] but given as " + decayRate);
            }
            coordinateStartIndex = 5;

            //expressionExecutors[2] --> numberOfClusters
            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("numberOfClusters has to be a constant but found " +
                        this.attributeExpressionExecutors[2].getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                numberOfClusters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppCreationException("numberOfClusters should be of type int but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

        } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
            decayRate = 0.01;
            if (logger.isDebugEnabled()) {
                logger.debug("Decay rate is not specified. using default " + decayRate);
            }
            coordinateStartIndex = 4;
            numberOfClusters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        } else {
            throw new SiddhiAppCreationException("The second query parameter should either be decayRate or " +
                    "numberOfClusters which should be of type double or int respectively but found " +
                    attributeExpressionExecutors[1].getReturnType());
        }

        //expressionExecutors[coordinateStartIndex-2] --> maxIterations
        if (!(attributeExpressionExecutors[coordinateStartIndex - 2] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppCreationException("Maximum iterations has to be a constant but found " +
                    this.attributeExpressionExecutors[coordinateStartIndex - 2].getClass().getCanonicalName());
        }
        int maxIterations;
        if (attributeExpressionExecutors[coordinateStartIndex - 2].getReturnType() == Attribute.Type.INT) {
            maxIterations = (Integer) ((ConstantExpressionExecutor)
                    attributeExpressionExecutors[coordinateStartIndex - 2]).getValue();
        } else {
            throw new SiddhiAppCreationException("Maximum iterations should be of type int but found " +
                    attributeExpressionExecutors[coordinateStartIndex - 2].getReturnType());
        }

        //expressionExecutors[coordinateStartIndex-1] --> numberOfEventsToRetrain
        if (!(attributeExpressionExecutors[coordinateStartIndex - 1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppCreationException("numberOfEventsToRetrain has to be a constant but found " +
                    this.attributeExpressionExecutors[coordinateStartIndex - 1].getClass().getCanonicalName());
        }
        if (attributeExpressionExecutors[coordinateStartIndex - 1].getReturnType() == Attribute.Type.INT) {
            numberOfEventsToRetrain = (Integer) ((ConstantExpressionExecutor)
                    attributeExpressionExecutors[coordinateStartIndex - 1]).getValue();
            if (numberOfEventsToRetrain <= 0) {
                throw new SiddhiAppCreationException("numberOfEventsToRetrain should be a positive integer " +
                        "but found " + numberOfEventsToRetrain);
            }
        } else {
            throw new SiddhiAppCreationException("numberOfEventsToRetrain should be of type int but found " +
                    attributeExpressionExecutors[coordinateStartIndex - 1].getReturnType());
        }

        dimensionality = attributeExpressionExecutors.length - coordinateStartIndex;
        coordinateValuesOfCurrentDataPoint = new double[dimensionality];

        //validating all the features
        featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                attributeExpressionExecutors, coordinateStartIndex, dimensionality);

        String siddhiAppName = siddhiAppContext.getName();
        modelName = modelName + "." + siddhiAppName;
        if (logger.isDebugEnabled()) {
            logger.debug("model name is " + modelName);
        }
        clusterer = new Clusterer(numberOfClusters, maxIterations, modelName, siddhiAppName, dimensionality);

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
                        throw new SiddhiAppCreationException("coordinate values should be int/float/double/long " +
                                "but found " +
                                attributeExpressionExecutors[i].execute(streamEvent).getClass());
                    }
                }

                //creating a dataPoint with the received coordinate values
                DataPoint currentDataPoint = new DataPoint();
                currentDataPoint.setCoordinates(coordinateValuesOfCurrentDataPoint);
                dataPointsArray.add(currentDataPoint);

                //handling the training
                if (numberOfEventsReceived % numberOfEventsToRetrain == 0) {
                    clusterer.train(new LinkedList<>(dataPointsArray), numberOfEventsToRetrain, decayRate,
                            executorService);
                    dataPointsArray.clear();
                }

                isModelInitialTrained = clusterer.isModelInitialTrained();
                if (isModelInitialTrained) {
                    logger.debug("Populating output");
                    complexEventPopulater.populateComplexEvent(streamEvent,
                            clusterer.getAssociatedCentroidInfo(currentDataPoint));
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
        KMeansModelHolder.getInstance().deleteKMeansModel(modelName);
    }

    @Override
    public Map<String, Object> currentState() {
        synchronized (this) {
            Map<String, Object> map = new HashMap();
            map.put("untrainedData", dataPointsArray);
            map.put("isModelInitialTrained", isModelInitialTrained);
            map.put("numberOfEventsReceived", numberOfEventsReceived);
            map.put("kMeansModelMap", KMeansModelHolder.getInstance().getClonedKMeansModelMap());
            logger.debug("storing kmeans modelmap " + map.get("kMeansModelMap"));
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            dataPointsArray = (LinkedList<DataPoint>) map.get("untrainedData");
            isModelInitialTrained = (Boolean) map.get("isModelInitialTrained");
            numberOfEventsReceived = (Integer) map.get("numberOfEventsReceived");
            Map<String, KMeansModel> modelMap = (Map<String, KMeansModel>) map.get("kMeansModelMap");
            KMeansModelHolder.getInstance().setKMeansModelMap(modelMap);
            clusterer.setModel(modelMap.get(modelName));
            clusterer.setModelInitialTrained(isModelInitialTrained);
        }
    }
}
