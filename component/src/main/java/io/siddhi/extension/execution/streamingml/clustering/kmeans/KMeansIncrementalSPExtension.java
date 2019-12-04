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

package io.siddhi.extension.execution.streamingml.clustering.kmeans;

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
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.execution.streamingml.clustering.kmeans.util.DataPoint;
import io.siddhi.extension.execution.streamingml.clustering.kmeans.util.KMeansClusterer;
import io.siddhi.extension.execution.streamingml.clustering.kmeans.util.KMeansModel;
import io.siddhi.extension.execution.streamingml.util.CoreUtils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.log4j.Logger;

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
                "All data points to be processed by a query should be of the same " +
                "dimensionality. The Euclidean distance is taken as the distance metric. " +
                "The algorithm resembles Sequential K-Means Clustering at " +
                "https://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm ",
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
                        name = "model.feature",
                        description = "This is a variable length argument. Depending on the dimensionality of data " +
                                "points we will receive coordinates as features along each axis.",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG},
                        dynamic = true
                )
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"no.of.clusters", "model.feature", "..."}),
                @ParameterOverload(parameterNames = {"no.of.clusters", "decay.rate", "model.feature", "..."})
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
                        syntax = "define stream InputStream (x double, y double);\n" +
                                "@info(name = 'query1')\n" +
                                "from InputStream#streamingml:kMeansIncremental(2, 0.2, x, y)\n" +
                                "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y\n" +
                                "insert into OutputStream;",
                        description = "This is an example where user provides the decay rate. First two events will " +
                                "be used to initiate the model since the required number of clusters is specified as " +
                                "2. After the first event itself prediction would start."
                ),
                @Example(
                        syntax = "define stream InputStream (x double, y double);\n" +
                                "@info(name = 'query1')\n" +
                                "from InputStream#streamingml:kMeansIncremental(2, x, y)\n" +
                                "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y\n" +
                                "insert into OutputStream;",
                        description = "This is an example where user doesnt give the decay rate so the default " +
                                "value will be used"
                )
        }
)
public class KMeansIncrementalSPExtension extends StreamProcessor<KMeansIncrementalSPExtension.ExtensionState> {
    private double decayRate = 0.01;
    private double[] coordinateValuesOfCurrentDataPoint;
    private int numberOfClusters;
    private int dimensionality;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new LinkedList<>();
    private static final Logger logger = Logger.getLogger(KMeansIncrementalSPExtension.class.getName());
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
        final int minConstantParams = 1;
        final int maxConstantParams = 2;
        final int minNumberOfFeatures = 1;
        int coordinateStartIndex;
        int maxNoOfFeatures = inputDefinition.getAttributeList().size();
        LinkedList<DataPoint> dataPoints = new LinkedList<>();

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
        KMeansModel kMeansModel = new KMeansModel();
        attributes = new ArrayList<>(1 + dimensionality);
        attributes.add(new Attribute("euclideanDistanceToClosestCentroid", Attribute.Type.DOUBLE));
        for (int i = 1; i <= dimensionality; i++) {
            attributes.add(new Attribute("closestCentroidCoordinate" + i, Attribute.Type.DOUBLE));
        }
        return () -> new ExtensionState(kMeansModel, dataPoints);
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk,
                           Processor processor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater,
                           ExtensionState extensionState) {
        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                StreamEvent streamEvent = complexEventChunk.next();
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
                extensionState.dataPoints.add(currentDataPoint);

                if (extensionState.kMeansModel.isTrained()) {
                    complexEventPopulater.populateComplexEvent(streamEvent,
                            KMeansClusterer.getAssociatedCentroidInfo(currentDataPoint, extensionState.kMeansModel));
                }

                //handling the training
                KMeansClusterer.train(
                        extensionState.dataPoints, 1, decayRate, null,
                        extensionState.kMeansModel, numberOfClusters, 2, dimensionality);
                extensionState.dataPoints.clear();
            }
        }
        nextProcessor.process(complexEventChunk);
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributes;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    static class ExtensionState extends State {
        private static final String KEY_UNTRAINED_DATA = "untrainedData";
        private static final String KEY_K_MEANS_MODEL = "kMeansModel";
        private KMeansModel kMeansModel;
        private LinkedList<DataPoint> dataPoints;

        private ExtensionState(KMeansModel kMeansModel, LinkedList<DataPoint> dataPoints) {
            this.dataPoints = dataPoints;
            this.kMeansModel = kMeansModel;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public synchronized Map<String, Object> snapshot() {
            Map<String, Object> state =  new HashMap<>();
            state.put(KEY_UNTRAINED_DATA, dataPoints);
            state.put(KEY_K_MEANS_MODEL, kMeansModel);
            logger.debug("storing kmeans model " + state.get(KEY_K_MEANS_MODEL));
            return state;
        }

        @Override
        public synchronized void restore(Map<String, Object> map) {
            dataPoints = (LinkedList<DataPoint>) map.get(KEY_UNTRAINED_DATA);
            kMeansModel = (KMeansModel) map.get(KEY_K_MEANS_MODEL);
        }
    }
}
