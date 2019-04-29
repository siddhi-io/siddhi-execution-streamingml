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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
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
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.DataPoint;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.KMeansClusterer;
import org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util.KMeansModel;
import org.wso2.extension.siddhi.execution.streamingml.util.CoreUtils;

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
public class KMeansMiniBatchSPExtension extends StreamProcessor<KMeansMiniBatchSPExtension.ExtensionState> {
    private double decayRate = 0.01;
    private int numberOfEventsToRetrain = 20;
    private double[] coordinateValuesOfCurrentDataPoint;
    private int maximumIterations = 50;
    private int numberOfClusters;
    private int dimensionality;
    private ExecutorService executorService;
    private List<VariableExpressionExecutor> featureVariableExpressionExecutors = new LinkedList<>();
    private static final Logger logger = Logger.getLogger(KMeansMiniBatchSPExtension.class.getName());

    private List<Attribute> attributes;

    @Override
    public void start() {
    }

    @Override
    public void stop() {
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
                extensionState.incKeyNumberOfEventsReceived();

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
                    logger.debug("Populating output");
                    complexEventPopulater.populateComplexEvent(streamEvent,
                            KMeansClusterer.getAssociatedCentroidInfo(currentDataPoint, extensionState.kMeansModel));
                }

                //handling the training
                if (extensionState.getNumberOfEventsReceived() % numberOfEventsToRetrain == 0) {
                    KMeansClusterer.train(
                            new LinkedList<>(extensionState.dataPoints), numberOfEventsToRetrain, decayRate,
                            executorService, extensionState.kMeansModel, numberOfClusters, maximumIterations,
                            dimensionality);
                    extensionState.dataPoints.clear();
                }

            }
        }
        nextProcessor.process(complexEventChunk);
    }

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
        final int maxConstantParams = 4;
        final int minNumberOfFeatures = 1;
        int coordinateStartIndex;
        int maxNoOfFeatures = inputDefinition.getAttributeList().size();
        LinkedList<DataPoint> dataPoints = new LinkedList<>();

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

        KMeansModel kMeansModel = new KMeansModel();

        executorService = siddhiQueryContext.getSiddhiAppContext().getExecutorService();

        attributes = new ArrayList<>(1 + dimensionality);
        attributes.add(new Attribute("euclideanDistanceToClosestCentroid", Attribute.Type.DOUBLE));
        for (int i = 1; i <= dimensionality; i++) {
            attributes.add(new Attribute("closestCentroidCoordinate" + i, Attribute.Type.DOUBLE));
        }
        return () -> new ExtensionState(kMeansModel, dataPoints);
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributes;

    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    static class ExtensionState extends State {

        private static final String KEY_UNTRAINED_DATA = "untrainedData";
        private static final String KEY_K_MEANS_MODEL = "kMeansModel";
        private static final String KEY_NUMBER_OF_EVENTS_RECEIVED = "numberOfEventsReceived";
        private final Map<String, Object> state;
        private KMeansModel kMeansModel;
        private LinkedList<DataPoint> dataPoints;
        private int numberOfEventsReceived;

        private ExtensionState(KMeansModel kMeansModel, LinkedList<DataPoint> dataPoints) {
            state = new HashMap<>();
            this.dataPoints = dataPoints;
            this.kMeansModel = kMeansModel;
            numberOfEventsReceived = 0;
        }

        private synchronized int getNumberOfEventsReceived() {
            return numberOfEventsReceived;
        }

        private synchronized void incKeyNumberOfEventsReceived() {
            numberOfEventsReceived++;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public synchronized Map<String, Object> snapshot() {
            state.put(KEY_UNTRAINED_DATA, dataPoints);
            state.put(KEY_K_MEANS_MODEL, kMeansModel);
            state.put(KEY_NUMBER_OF_EVENTS_RECEIVED, numberOfEventsReceived);
            return state;
        }

        @Override
        public synchronized void restore(Map<String, Object> map) {
            dataPoints = (LinkedList<DataPoint>) map.get(KEY_UNTRAINED_DATA);
            kMeansModel = (KMeansModel) map.get(KEY_K_MEANS_MODEL);
            numberOfEventsReceived = (Integer) map.get(KEY_NUMBER_OF_EVENTS_RECEIVED);
        }
    }
}
