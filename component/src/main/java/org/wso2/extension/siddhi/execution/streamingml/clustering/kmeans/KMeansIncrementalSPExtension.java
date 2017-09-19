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

/**
 * performs kmeans with incremental update
 */
@Extension(
        name = "kMeansIncremental",
        namespace = "streamingml",
        description = "Performs K-Means clustering on a streaming data set. Data points can be of any dimension and " +
                "the dimensionality is calculated from number of parameters. " +
                "All data points to be processed by an instance of class Clusterer should be of the same " +
                "dimensionality. The Euclidean distance is taken as the distance metric. " +
                "The algorithm resembles Sequential K-Means Clustering at " +
                "https://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm ",
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
                        defaultValue = "0.01f"
                ),
                @Parameter(
                        name = "no.of.clusters",
                        description = "The assumed number of natural clusters (numberOfClusters) in the data set.",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "model.features",
                        description = "This is a variable length argument. Depending on the dimensionality of data " +
                                "points we will receive coordinates as features along each axis.",
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
                        description = "This is a variable length attribute. Depending on the dimensionality(D) " +
                                "we will return closestCentroidCoordinate1, closestCentroidCoordinate2,... " +
                                "closestCentroidCoordinateD which are the d dimensional coordinates of the closest " +
                                "centroid from the model to the current event. This is the prediction result and " +
                                "this represents the cluster to which the current event belongs to.",
                        type = {DataType.DOUBLE}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (modelFeature1 double, modelFeature2 double);" +
                                "from InputStream#streamingML:KMeansIncremental(modelName, numberOfClusters, " +
                                "modelFeature1, modelFeature2)\"\n" +
                                "select modelFeature1, modelFeature2, euclideanDistanceToClosestCentroid, " +
                                "closestCentroidCoordinate1, closestCentroidCoordinate2\"\n" +
                                "insert into OutputStream",
                        description = "modelName='model1', numberOfClusters=2, This will select first two " +
                                "distinct values in " +
                                "events as initial model and predict and update the model for every event"
                ),
        }
)
public class KMeansIncrementalSPExtension extends StreamProcessor {
    private double decayRate;
    private LinkedList<DataPoint> dataPointsArray;
    private double[] coordinateValuesOfCurrentDataPoint;
    private boolean isModelInitialTrained;
    private Clusterer clusterer;
    private int dimensionality;
    private String modelName;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new LinkedList<>();
    private static final Logger logger = Logger.getLogger(KMeansIncrementalSPExtension.class.getName());

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        dataPointsArray = new LinkedList<>();

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

        //expressionExecutors[1] --> numberOfClusters or decayRate
        int numberOfClusters;
        int coordinateStartIndex;
        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (logger.isDebugEnabled()) {
                    logger.debug("decayRate not specified. using default");
                }
                decayRate = 0.01;
                coordinateStartIndex = 2;
                numberOfClusters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppCreationException("numberOfClusters has to be a constant but found " +
                        this.attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (logger.isDebugEnabled()) {
                    logger.debug("decayRate is specified.");
                }
                decayRate = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();

                if (decayRate < 0 || decayRate > 1) {
                    throw new SiddhiAppCreationException("decayRate should be in [0,1] but given as " + decayRate);
                }

                //expressionExecutors[2] --> numberOfClusters
                if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                    throw new SiddhiAppCreationException("numberOfClusters has to be a constant but found " +
                            this.attributeExpressionExecutors[2].getClass().getCanonicalName());
                }
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    coordinateStartIndex = 3;
                    numberOfClusters = (Integer) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new SiddhiAppCreationException("numberOfClusters should be of type int but found " +
                            attributeExpressionExecutors[2].getReturnType());
                }
            } else {
                throw new SiddhiAppCreationException("decayRate has to be a constant but found " +
                        this.attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppCreationException("The second query parameter should either be decayRate or " +
                    "numberOfClusters which should be of type double or int respectively but found " +
                    attributeExpressionExecutors[1].getReturnType());
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

        clusterer = new Clusterer(numberOfClusters, 2, modelName, siddhiAppName, dimensionality);

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
                clusterer.train(dataPointsArray, 1, decayRate, null);
                dataPointsArray.clear();

                isModelInitialTrained = clusterer.isModelInitialTrained();
                if (isModelInitialTrained) {
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
            map.put("kMeansModel", KMeansModelHolder.getInstance().getClonedKMeansModel(modelName));
            logger.debug("storing kmeans model " + map.get("kMeansModel"));
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            dataPointsArray = (LinkedList<DataPoint>) map.get("untrainedData");
            isModelInitialTrained = (Boolean) map.get("isModelInitialTrained");
            KMeansModel model = (KMeansModel) map.get("kMeansModel");
            KMeansModelHolder.getInstance().addKMeansModel(modelName, model);
            clusterer.setModel(model);
            clusterer.setModelInitialTrained(isModelInitialTrained);
        }
    }
}
