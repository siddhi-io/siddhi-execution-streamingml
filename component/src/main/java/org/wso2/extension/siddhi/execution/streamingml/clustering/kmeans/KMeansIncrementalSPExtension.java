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
        description = "This function performs K-Means clustering on a streaming data set. " +
                "Data points can be of any dimension and " +
                "the dimensionality is calculated from the number of parameters. " +
                "All the data points that are processed should be of the same " +
                "dimensionality. The Euclidean distance is taken as the distance metric. " +
                "The algorithm resembles the Sequential K-Means Clustering. See the link given below for " +
                "further details on this.\n " +
                "https://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm ",
        parameters = {
                @Parameter(
                        name = "no.of.clusters",
                        description = "The assumed number of natural clusters in the data set.",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "decay.rate",
                        description = "This is the decay rate of the old data compared to that of the new data. " +
                                "The value is shown as [0,1]. 0 indicates that only old data is used and" +
                                "1 indicates that only new data is used.",
                        optional = true,
                        type = {DataType.DOUBLE},
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "model.features",
                        description = "This is a variable length argument. Depending on the dimensionality of the " +
                                "data points, we receive coordinates as features along each axis.",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG}
                )

        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "euclideanDistanceToClosestCentroid",
                        description = "This represents the Euclidean distance between the current data point and the " +
                                "closest centroid.",
                        type = {DataType.DOUBLE}
                ),
                @ReturnAttribute(
                        name = "closestCentroidCoordinate",
                        description = "This is a variable length attribute. Depending on the dimensionality(D) " +
                                "the function returns values as closestCentroidCoordinate1, " +
                                "closestCentroidCoordinate2,...closestCentroidCoordinateD," +
                                " which are the d dimensional coordinates of the closest " +
                                "centroid from the model to the current event. This is the prediction result and " +
                                "this represents the cluster to which the current event belongs.",
                        type = {DataType.DOUBLE}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (x double, y double);\n" +
                                "@info(name = 'query1')\n" +
                                "from InputStream#streamingml:kMeansIncremental(2, 0.2, x, y)\n" +
                                "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y\n" +
                                "insert into OutputStream;",
                        description = "This is an example where user has provided the decay rate. " +
                                "First two events are used to initiate the model since the required" +
                                " number of clusters is specified as " +
                                "two. However, the prediction starts after the first event itself."
                ),
                @Example(
                        syntax = "define stream InputStream (x double, y double);\n" +
                                "@info(name = 'query1')\n" +
                                "from InputStream#streamingml:kMeansIncremental(2, x, y)\n" +
                                "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y\n" +
                                "insert into OutputStream;",
                        description = "This is an example where the user has not given the decay rate, hence " +
                                "the default value is used."
                )
        }
)
public class KMeansIncrementalSPExtension extends StreamProcessor {
    private double decayRate = 0.01;
    private LinkedList<DataPoint> dataPointsArray;
    private double[] coordinateValuesOfCurrentDataPoint;
    private KMeansModel kMeansModel;
    private int numberOfClusters;
    private int dimensionality;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new LinkedList<>();
    private static final Logger logger = Logger.getLogger(KMeansIncrementalSPExtension.class.getName());

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        final int minConstantParams = 1;
        final int maxConstantParams = 2;
        final int minNumberOfFeatures = 1;
        int coordinateStartIndex;
        int maxNoOfFeatures = inputDefinition.getAttributeList().size();
        dataPointsArray = new LinkedList<>();

        if (attributeExpressionLength < minConstantParams + minNumberOfFeatures ||
                attributeExpressionLength > maxConstantParams + maxNoOfFeatures) {
            throw new SiddhiAppCreationException("Invalid number of parameters. Please check the query");
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
            coordinateStartIndex = 2;
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
        }

        dimensionality = attributeExpressionLength - coordinateStartIndex;
        coordinateValuesOfCurrentDataPoint = new double[dimensionality];

        //validating all the features
        featureVariableExpressionExecutors = CoreUtils.extractAndValidateFeatures(inputDefinition,
                attributeExpressionExecutors, coordinateStartIndex, dimensionality);

        kMeansModel = new KMeansModel();

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

                if (kMeansModel.isTrained()) {
                    complexEventPopulater.populateComplexEvent(streamEvent,
                            KMeansClusterer.getAssociatedCentroidInfo(currentDataPoint, kMeansModel));
                }

                //handling the training
                KMeansClusterer.train(dataPointsArray, 1, decayRate, null, kMeansModel,
                        numberOfClusters, 2, dimensionality);
                dataPointsArray.clear();
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
            map.put("kMeansModel", kMeansModel);
            logger.debug("storing kmeans model " + map.get("kMeansModel"));
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            dataPointsArray = (LinkedList<DataPoint>) map.get("untrainedData");
            kMeansModel = (KMeansModel) map.get("kMeansModel");
        }
    }
}
