package org.wso2.extension.siddhi.execution.ml.kmeansFromScratch;

/**
 * Created by niruhan on 7/11/17.
 */

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
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
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

@Extension(
        name = "cluster",
        namespace = "kmeans",
        description = "Performs single dimension clustering on a given data set for a given window implementation." +
                "For example: #window.length(10)#kmeans:cluster(value, 4, 20, train)" +
                "The model is trained for the number of events specified or when true is sent for an event. " +
                "The training process initializes the first k distinct" +
                "events in the window as initial centroids. The data are assigned to a centroid based on the " +
                "nearest distance. If a data point is equidistant to two centroids, it is assigned to the centroid" +
                " with the higher center value",
        parameters = {
                @Parameter(name = "value",
                        description = "Value to be clustered",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "cluster.centers",
                        description = "Number of cluster centers",
                        type = {DataType.INT}),
                @Parameter(name = "iterations",
                        description = "Number of iterations, the process iterates until the number of maximum " +
                                "iterations is reached or the centroids do not change",
                        type = DataType.INT),
                @Parameter(name = "events.to.train",
                        description = "New cluster centers are found for given number of events",
                        optional = true,
                        type = DataType.INT),
                @Parameter(name = "train",
                        optional = true,
                        description = "train the model for available amount of data",
                        type = DataType.BOOL)

        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "matchedClusterCentroid",
                        description = "Returns cluster center to which data point belongs to",
                        type = {DataType.DOUBLE}),
                @ReturnAttribute(
                        name = "matchedClusterIndex",
                        description = "the index of the cluster center",
                        type = {DataType.INT}
                ),
                @ReturnAttribute(
                        name = "distanceToCenter",
                        description = "the difference between the value and the cluster center",
                        type = {DataType.DOUBLE}
                )
        },
        examples = {@Example(syntax = "\"from InputStream#window.length(5)#kmeans:cluster(value, 4, 20, 5) \"\n" +
                " select value, matchedClusterCentroid, matchedClusterIndex, distanceToCenter \"\n" +
                " insert into OutputStream;",
                description = "This will cluster the collected values within the window for every 5 events" +
                        "and give the output after the first 5 events."),
                @Example(syntax = "from InputStream#window.length(10)#kmeans:cluster(value, 4, 20, train) "
                        + "select value, matchedClusterCentroid, matchedClusterIndex, distanceToCenter "
                        + "insert into OutputStream;",
                        description = "This will cluster the current values within the window when true is received " +
                                "in the event stream, for a particular event where train should be a boolean")}
)
public class KMeans extends StreamProcessor {
    private int k;
    private int maxIterations;

    private int numberOfEventsToRetrain;
    private int numberOfEventsReceived;
    private ArrayList<DataPoint> dataPointsArray = new ArrayList<>();

    private boolean needToCheckTrainNow = false;
    private boolean modelTrained = false;
    private Clusterer clusterer;
    private int dimensionality;
    private double[] coordinateValues;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();

            if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                numberOfEventsReceived++;
                coordinateValues = new double[dimensionality];

                //validating and getting coordinate values
                for (int i=4; i<4+dimensionality; i++) {
                    //validate the coordinate values of the dataPoint
                    if (!(attributeExpressionExecutors[i] instanceof ConstantExpressionExecutor)) {
                        throw new SiddhiAppValidationException("Coordinate values of data points should be constant");
                    }
                    Object content = attributeExpressionExecutors[i].execute(streamEvent);
                    if (content instanceof Double) {
                        coordinateValues[i-4] = (Double) content;
                    } else {
                        throw new SiddhiAppValidationException("Coordinate values of data point should be " +
                                "of type double but found " + attributeExpressionExecutors[i].getReturnType());
                    }
                }

                //creating a dataPoint with the received coordinate values
                DataPoint currentDataPoint = new DataPoint(dimensionality);
                currentDataPoint.setCoordinates(coordinateValues);
                dataPointsArray.add(currentDataPoint);

                //handling the training
                if (needToCheckTrainNow) {
                    boolean trainNow = (Boolean) attributeExpressionExecutors[3].execute(streamEvent);
                    if (trainNow) {
                        clusterer.cluster(dataPointsArray);
                        modelTrained = true;
                    }
                } else if (numberOfEventsToRetrain > 0) {
                    if (numberOfEventsReceived % numberOfEventsToRetrain == 0) {
                        clusterer.cluster(dataPointsArray);
                        modelTrained = true;
                    }
                }

                if (modelTrained) {
                    Object[] outputData;
                    //outputData = clusterer.g
                }
            }
        }
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        //expressionExecutors[0] --> dimensionality
        if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Dimensionality has to be a constant.");
        }
        Object zerothContent = attributeExpressionExecutors[0].execute(null);
        if (zerothContent instanceof Integer) {
            dimensionality = (Integer) zerothContent;
        } else {
            throw new SiddhiAppValidationException("Dimensionality should be of type int but found " +
                    attributeExpressionExecutors[0].getReturnType());
        }

        //expressionExecutors[1] --> k
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("k has to be a constant.");
        }
        Object firstContent = attributeExpressionExecutors[1].execute(null);
        if (firstContent instanceof Integer) {
            k = (Integer) firstContent;
        } else {
            throw new SiddhiAppValidationException("k should be of type int but found " +
                    attributeExpressionExecutors[1].getReturnType());
        }

        //expressionExecutors[2] --> maxIterations
        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Maximum iterations has to be a constant.");
        }
        Object secondContent = attributeExpressionExecutors[2].execute(null);
        if (secondContent instanceof Integer) {
            maxIterations = (Integer) secondContent;
        } else {
            throw new SiddhiAppValidationException("Maximum iterations should be of type int but found " +
                    attributeExpressionExecutors[2].getReturnType());
        }

        //expressionExecutors[3] --> trainModel or numberOfEventsToRetrain
        if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.BOOL) {
            needToCheckTrainNow = true;
        } else if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
            needToCheckTrainNow = false;
            Object thirdContent = attributeExpressionExecutors[3].execute(null);
            if (thirdContent instanceof Integer) {
                numberOfEventsToRetrain = (Integer) thirdContent;
            } else {
                throw new SiddhiAppValidationException("Number of events to trigger retraining" +
                        "should be of type int but found " +attributeExpressionExecutors[3].getReturnType());
            }
        } else {
            throw new SiddhiAppValidationException("The 4th parameter should either be boolean or int but found" +
                    attributeExpressionExecutors[3].getReturnType());
        }

        //validate the length of expressionExecutors[]
        if (!(attributeExpressionExecutors.length == 4+dimensionality)) {
            throw new SiddhiAppValidationException("Clustering function should have exactly " +
                    (4+dimensionality) +" parameters, currently " + attributeExpressionExecutors.length
            + " parameters provided");
        }

        clusterer = new Clusterer(k,maxIterations);

        List<Attribute> attributeList = new ArrayList<>(1+dimensionality);
        attributeList.add(new Attribute("euclideanDistanceToClosestCentroid", Attribute.Type.DOUBLE));
        for (int i=1; i<=dimensionality; i++) {
            attributeList.add(new Attribute("closestCentroidCoordinate"+i, Attribute.Type.DOUBLE));
        }
        return attributeList;
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
        map.put("data", dataPointsArray);
        map.put("modelTrained", modelTrained);
        map.put("numberOfEventsReceived", numberOfEventsReceived);
        return map;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        dataPointsArray = (ArrayList<DataPoint>) map.get("data");
        modelTrained = (Boolean) map.get("modelTrained");
        numberOfEventsReceived = (Integer) map.get("numberOfEventsReceived");
    }


}

