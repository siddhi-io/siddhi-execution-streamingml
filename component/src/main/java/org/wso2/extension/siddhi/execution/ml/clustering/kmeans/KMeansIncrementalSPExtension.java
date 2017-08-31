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

package org.wso2.extension.siddhi.execution.ml.clustering.kmeans;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.Clusterer;
import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.DataPoint;
import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.KMeansModel;
import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.KMeansModelHolder;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


@Extension(
        name = "KMeansIncremental",
        namespace = "streamingML",
        description = "Performs K-Means clustering on a streaming data set. Data points can be of any dimension and the dimensionality is calculated from number of parameters. " +
                "All data points to be processed by an instance of class Clusterer should be of the same dimensionality. The Euclidean distance is taken as the distance metric. " +
                "The algorithm resembles Sequential numberOfClusters-Means Clustering at https://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm " +
                "For example: #streamingML:KMeansIncremental(modelName, numberOfClusters, x1, x2, .... , xd)" +
                "Model is trained for every event. The training process initializes the first numberOfClusters distinct events in the window as" +
                "initial centroids. The dataPoints are assigned to the respective closest centroid.",
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
                        type = {DataType.FLOAT},
                        defaultValue = "0.01f"
                ),
                @Parameter(
                        name = "number.of.clusters",
                        description = "The assumed number of natural clusters (numberOfClusters) in the data set.",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "coordinate.values",
                        description = "This is a variable length argument. Depending on the dimensionality of data points we will receive coordinates along each axis.",
                        type = {DataType.DOUBLE} //how to give multiple types?
                )

        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "euclideanDistanceToClosestCentroid",
                        description = "Represents the Euclidean distance between the current data point and the closest centroid.",
                        type = {DataType.DOUBLE}
                ),
                @ReturnAttribute(
                        name = "closestCentroidCoordinatei",
                        description = "This is a variable length attribute. Depending on the dimensionality(d) we will return closestCentroidCoordinate1 to closestCentroidCoordinated",
                        type = {DataType.DOUBLE}
                )
        },
        examples = {
                @Example(
                        syntax = "from InputStream#streamingML:KMeansIncremental(modelName, numberOfClusters, coordinateValue1, coordinateValue2)\"\n" +
                                "select coordinateValue1, coordinateValue2, euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, closestCentroidCoordinate2\"\n" +
                                "insert into OutputStream",
                        description = "modelName='model1', numberOfClusters=2, This will select first two distinct values in " +
                                "events as initial model and predict and update the model for every event"
                ),
        }
)
public class KMeansIncrementalSPExtension extends StreamProcessor {
    private float decayRate;


    private int numberOfEventsReceived;
    private int coordinateStartIndex;
    private LinkedList<DataPoint> dataPointsArray;

    private boolean modelTrained = false;
    private Clusterer clusterer;
    private int dimensionality;
    private final static Logger logger = Logger.getLogger(KMeansIncrementalSPExtension.class.getName());

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        dataPointsArray = new LinkedList<>();

        //expressionExecutors[0] --> modelName
        if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("modelName has to be a constant.");
        }
        String modelName;
        if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
            modelName = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
        } else {
            throw new SiddhiAppValidationException("modelName should be of type String but found " +
                    attributeExpressionExecutors[0].getReturnType());
        }

        //expressionExecutors[1] --> numberOfClusters or decayRate
        int numberOfClusters;
        if (attributeExpressionExecutors[1].getReturnType() ==  Attribute.Type.INT) {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                logger.debug("decayRate not specified. using default");
                decayRate=0.01f;
                coordinateStartIndex = 2;
                numberOfClusters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppValidationException("numberOfClusters has to be a constant.");
            }
        } else if (attributeExpressionExecutors[1].getReturnType() ==  Attribute.Type.FLOAT) {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                logger.debug("decayRate is specified.");
                decayRate = (Float) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();

                //expressionExecutors[2] --> numberOfClusters
                if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                    throw new SiddhiAppValidationException("numberOfClusters has to be a constant.");
                }
                if (attributeExpressionExecutors[2].getReturnType() ==  Attribute.Type.INT) {
                    coordinateStartIndex = 3;
                    numberOfClusters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new SiddhiAppValidationException("numberOfClusters should be of type int but found " +
                            attributeExpressionExecutors[2].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("decayRate has to be a constant.");
            }
        } else {
            throw new SiddhiAppValidationException("The second query parameter should either be decayRate or numberOfClusters which should be of type float or int respectively but found " +
            attributeExpressionExecutors[1].getReturnType());
        }

        dimensionality = attributeExpressionExecutors.length - coordinateStartIndex;

        //validating all the attributes to be variables
        for (int i=coordinateStartIndex; i<coordinateStartIndex+dimensionality; i++) {
            if (!(this.attributeExpressionExecutors[i] instanceof VariableExpressionExecutor)) {
                throw new SiddhiAppValidationException("The attributes should be variable but found a " +
                        this.attributeExpressionExecutors[i].getClass().getCanonicalName());
            }
        }

        String siddhiAppName = siddhiAppContext.getName();
        modelName = modelName +"."+siddhiAppName;
        logger.debug("model name is " +modelName);
        int maxIterations = 2;
        clusterer = new Clusterer(numberOfClusters, maxIterations, modelName, siddhiAppName, dimensionality);

        List<Attribute> attributeList = new ArrayList<>(1+dimensionality);
        attributeList.add(new Attribute("euclideanDistanceToClosestCentroid", Attribute.Type.DOUBLE));
        for (int i=1; i<=dimensionality; i++) {
            attributeList.add(new Attribute("closestCentroidCoordinate"+i, Attribute.Type.DOUBLE));
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
                double[] coordinateValues = new double[dimensionality];


                //validating and getting coordinate values
                for (int i = coordinateStartIndex; i < coordinateStartIndex + dimensionality; i++) {
                    try {
                        Number content = (Number) attributeExpressionExecutors[i].execute(streamEvent);
                        coordinateValues[i - coordinateStartIndex] = content.doubleValue();
                    } catch (ClassCastException e) {
                        throw new SiddhiAppValidationException("coordinate values should be int/float/double/long but found " +
                                attributeExpressionExecutors[i].execute(streamEvent).getClass());
                    }
                }

                //creating a dataPoint with the received coordinate values
                DataPoint currentDataPoint = new DataPoint();
                currentDataPoint.setCoordinates(coordinateValues);
                dataPointsArray.add(currentDataPoint);

                //handling the training
                clusterer.train(dataPointsArray, 1, decayRate, null);
                dataPointsArray.clear();

                modelTrained = clusterer.isModelTrained();
                if (modelTrained) {
                    complexEventPopulater.populateComplexEvent(streamEvent, clusterer.getAssociatedCentroidInfo(currentDataPoint));
                } else {
                    streamEventChunk.remove();
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
        Map<String, Object> map = new HashMap();
        map.put("untrainedData", dataPointsArray);
        map.put("modelTrained", modelTrained);
        map.put("numberOfEventsReceived", numberOfEventsReceived);
        map.put("model", KMeansModelHolder.getInstance().getClonedKMeansModelMap());
        return map;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        dataPointsArray = (LinkedList<DataPoint>) map.get("untrainedData");
        modelTrained = (Boolean) map.get("modelTrained");
        numberOfEventsReceived = (Integer) map.get("numberOfEventsReceived");
        KMeansModel model1 = (KMeansModel) map.get("model");
        clusterer.setModel(model1);
    }


}

