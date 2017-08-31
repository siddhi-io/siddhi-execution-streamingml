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
import java.util.concurrent.ExecutorService;


@Extension(
        //TODO: namesapece - small. name - camelcase - done
        name = "KMeansMiniBatch", //TODO: use style sheet
        namespace = "streamingml",
        description = "Performs K-Means clustering on a streaming data set. Data points can be of " +
                "any dimension and the dimensionality is calculated from number of parameters. " +
                "All data points to be processed in a single query should be of the" + //tODO: use query for clusterer - done
                " same dimensionality. The Euclidean distance is taken as the distance metric. " +
                "The algorithm resembles mini-batch K-Means. (refer Web-Scale K-Means Clustering by " +
                "D.Sculley, Google, Inc.). " +
                "For example: #streamingML:KMeansMiniBatch(dimensionality, numberOfClusters, maxIterations,",
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
                        name = "max.iterations",
                        description = "Number of iterations, the process iterates until the number of maximum iterations is reached or the centroids do not change",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "number.of.events.to.retrain",
                        description = "number of events to recalculate cluster centers. ", //TODO: check capitalizations. rukshan - ??
                        type = DataType.INT
                ),
                @Parameter(
                        name = "coordinate.values",
                        description = "This is a variable length argument. Depending on the dimensionality of data points we will receive coordinates along each axis.",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG} //todo: add more types after commas - done
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
                        syntax = "from InputStream#streamingml:kmeansminibatch(modelName, numberOfClusters, maxIterations," +
                                " numberOfEventsToRetrain, decayRate, coordinateValue1, coordinateValue2)\"\n" +
                                "select coordinateValue1, coordinateValue2, euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, closestCentroidCoordinate2\"\n" +
                                "insert into OutputStream", //TODO: change the query syntax - done
                        description = "modelName =model1, numberOfClusters=2, numberOfEventsToRetrain = 5, maxIterations=10" +
                                " decayRate=0.2. This will cluster the collected data points within the window for every 5 events" +
                                "and give output after the first 5 events. Retraining will also happen after every 5 events"
                ),
        }
)
public class KMeansMiniBatchSPExtension extends StreamProcessor {
    private float decayRate;


    private int numberOfEventsToRetrain;
    private int numberOfEventsReceived;
    private int coordinateStartIndex;
    private LinkedList<DataPoint> dataPointsArray; //TODO: use linkedlist or array - done
    //private double[] coordinateValuesOfCurrentDataPoint; //TODO: use class level array and reuse it. creating arrays for every event is expensive - error?


    private boolean modelTrained = false;
    private Clusterer clusterer;
    private int dimensionality;
    private ExecutorService executorService;
    private final static Logger logger = Logger.getLogger(KMeansMiniBatchSPExtension.class.getName());

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        //expressionExecutors[0] --> modelName
        dataPointsArray = new LinkedList<>();
        String modelName;
        int numberOfClusters;
        if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("modelName has to be a constant.");
        }
        if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) { //TODO: check return type - done
            modelName = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue(); //TODO; change .execute to .getvalue. see nirmals code - done
        } else {
            throw new SiddhiAppValidationException("modelName should be of type String but found " +
                    attributeExpressionExecutors[0].getReturnType());
        }

        //expressionExecutors[1] --> decayRate or numberOfClusters
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("2nd parameter can be decayRate/numberOfClusters. Both has to be a constant but found a variable in place.");
        }
        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.FLOAT) { //TODO: check return type - done
            if (logger.isDebugEnabled()) {
                logger.debug("Decay rate is specified." + siddhiAppContext.getName()); //TODO: no need to log. may be debug logger.use execution plan name also - done
            }
            decayRate = (Float) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue(); //TODO: check accepted range - done
            if (decayRate<0 || decayRate>1) {
                throw new SiddhiAppValidationException("decayRate should be in [0,1] but given as " + decayRate);
            }
            coordinateStartIndex = 5;

            //expressionExecutors[2] --> numberOfClusters
            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("numberOfClusters has to be a constant."); //TODO: use numberOfClusters - done?
            }
            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) { //TODO: check return type - done
                numberOfClusters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppValidationException("numberOfClusters should be of type int but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

        } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT){
            if (logger.isDebugEnabled()) {
                logger.debug("Decay rate is not specified. using default " + decayRate); //todo: use debug logger.specify value - done
            }
            decayRate = 0.01f;
            coordinateStartIndex = 4;
            numberOfClusters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        }else {
            throw new SiddhiAppValidationException("The second query parameter should either be decayRate or numberOfClusters which should be of type float or int respectively but found " +
                    attributeExpressionExecutors[1].getReturnType()); //TODO: explain more - done
        }


        //expressionExecutors[coordinateStartIndex-2] --> maxIterations
        if (!(attributeExpressionExecutors[coordinateStartIndex-2] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Maximum iterations has to be a constant.");
        }
        int maxIterations;
        if (attributeExpressionExecutors[coordinateStartIndex-2].getReturnType() == Attribute.Type.INT) { //todo: check return type - done
            maxIterations = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[coordinateStartIndex-2]).getValue();
        } else {
            throw new SiddhiAppValidationException("Maximum iterations should be of type int but found " +
                    attributeExpressionExecutors[coordinateStartIndex-2].getReturnType());
        }

        //expressionExecutors[coordinateStartIndex-1] --> numberOfEventsToRetrain
        if (!(attributeExpressionExecutors[coordinateStartIndex-1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("numberOfEventsToRetrain has to be a constant.");
        }
        if (attributeExpressionExecutors[coordinateStartIndex-1].getReturnType() == Attribute.Type.INT) {
            numberOfEventsToRetrain = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[coordinateStartIndex-1]).getValue();
            if (numberOfEventsToRetrain<=0) {
                throw new SiddhiAppValidationException("numberOfEventsToRetrain should be a positive integer but found " + numberOfEventsToRetrain);
            }
        } else {
            throw new SiddhiAppValidationException("numberOfEventsToRetrain should be of type int but found " +
                    attributeExpressionExecutors[coordinateStartIndex-1].getReturnType());
        }

        dimensionality = attributeExpressionExecutors.length - coordinateStartIndex;
        //coordinateValuesOfCurrentDataPoint = new double[dimensionality];

        //validating all the attributes to be variables
        for (int i=coordinateStartIndex; i<coordinateStartIndex+dimensionality; i++) {
            if (!(this.attributeExpressionExecutors[i] instanceof VariableExpressionExecutor)) {
                throw new SiddhiAppValidationException("The attributes should be variable but found a " +
                        this.attributeExpressionExecutors[i].getClass().getCanonicalName());
            }
        }

        String siddhiAppName = siddhiAppContext.getName();
        modelName = modelName +"."+siddhiAppName;
        logger.debug("model name is " +modelName); //TODO: add debug log to store if we reuse an existing model or creating new - done in clusterer
        clusterer = new Clusterer(numberOfClusters, maxIterations, modelName, siddhiAppName, dimensionality);

        executorService = siddhiAppContext.getExecutorService();
        //logger.setLevel(Level.ALL);

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
        //TODO: add synchronized block around inside process - done
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                //TODO: no need to check for current - done
                numberOfEventsReceived++;
                double[] coordinateValuesOfCurrentDataPoint = new double[dimensionality];//TODO: use class level array and reuse it. creating arrays for every event is expensive - error

                //validating and getting coordinate values
                for (int i = coordinateStartIndex; i < coordinateStartIndex + dimensionality; i++) { //TODO:validate the attributes to be variables in init - done
                    //Object content = ;

                    try {
                        Number content = (Number) attributeExpressionExecutors[i].execute(streamEvent);
                        coordinateValuesOfCurrentDataPoint[i - coordinateStartIndex] = content.doubleValue();
                    } catch (ClassCastException e) {
                        throw new SiddhiAppValidationException("coordinate values should be int/float/double/long but found " +
                                attributeExpressionExecutors[i].execute(streamEvent).getClass());
                    }
                }

                //creating a dataPoint with the received coordinate values
                DataPoint currentDataPoint = new DataPoint();
                currentDataPoint.setCoordinates(coordinateValuesOfCurrentDataPoint);
                dataPointsArray.add(currentDataPoint);

                //handling the training
                if (numberOfEventsReceived % numberOfEventsToRetrain == 0) {
                    clusterer.train(dataPointsArray, numberOfEventsToRetrain, decayRate, executorService);
                    dataPointsArray.clear();
                }
                //TODO: numberOfEventsToRetrain value validation in init - done

                modelTrained = clusterer.isModelTrained();
                if (modelTrained) {
                    complexEventPopulater.populateComplexEvent(streamEvent, clusterer.getAssociatedCentroidInfo(currentDataPoint)); //TODO: put both in same line - done
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

