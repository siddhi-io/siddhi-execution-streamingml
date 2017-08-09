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
        description = "Performs K-Means clustering on a streaming data set. Data points can be of any dimension and the dimensionality should be passed as a parameter. " +
                "All data points to be processed by an instance of class Clusterer should be of the same dimensionality. The Euclidean distance is taken as the distance metric. " +
                "The algorithm resembles mini-batch K-Means. (refer Web-Scale K-Means Clustering by D.Sculley, Google, Inc.). Supports a given size window implementation. " +
                "For example: #window.length(10)#kmeans:cluster(dimensionality, k, maxIterations, train, x1, x2, .... , xd)" +
                "Model is trained for every specified number of events or when true is passed as train parameter. The training process initializes the first k distinct events in the window as" +
                "initial centroids. The dataPoints are assigned to the respective closest centroid.",
        parameters = {
                @Parameter(
                        name = "dimensionality",
                        description = "The number of dimensions need to represent dataPoint. Needs to be constant for all events in a single stream.",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "k",
                        description = "The assumed number of natural clusters in the data set.",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "max.iterations",
                        description = "Number of iterations, the process iterates until the number of maximum iterations is reached or the centroids do not change",
                        type = {DataType.INT}
                ),
                @Parameter(
                        name = "number.of.events.to.retrain",
                        description = "New cluster centers are found for given number of events",
                        optional = true,
                        type = DataType.INT,
                        defaultValue = "5"
                ),
                @Parameter(
                        name = "train",
                        optional = true,
                        description = "train the model for available amount of data",
                        type = DataType.BOOL,
                        defaultValue = "false"
                ),
                @Parameter(
                        name = "decay.rate",
                        description = "this is the decay rate of old data compared to new data. " +
                                "Value of this will be in [0,1]. 0 means only old data used and" +
                                "1 will mean that only new data is used",
                        type = {DataType.FLOAT}
                ),
                @Parameter(
                        name = "coordinate.values",
                        description = "This is a variable length argument. Depending on the dimensionality of data points we will receive coordinates along each axis.",
                        type = {DataType.DOUBLE}
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
                        syntax = "from InputStream#window.length(5)#kmeans:cluster(dimensionality, k, maxIterations, numberOfEventsToRetrain, decayRate, coordinateValue1, coordinateValue2)\"\n" +
                                "select coordinateValue1, coordinateValue2, euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, closestCentroidCoordinate2\"\n" +
                                "insert into OutputStream",
                        description = "dimensionality =2, k=2, numberOfEventsToRetrain = 5, maxIterations=10. This will cluster the collected data points within the window for every 5 events" +
                                "and give output after the first 5 events"
                ),
                @Example(
                        syntax = "from InputStream#window.length(5)#kmeans:cluster(2, 2, 20, trainNow, decayRate, coordinateValue1, coordinateValue2)\"\n" +
                                "select coordinateValue1, coordinateValue2, euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, closestCentroidCoordinate2\"\n" +
                                "insert into OutputStream",
                        description = "This will cluster the collected data points within the window when true is received" +
                                "in the event stream, for any event trainNow should be a boolean"
                )
        }
)
public class KMeans extends StreamProcessor {
    private int k;
    private int maxIterations;
    private float decayRate;


    private int numberOfEventsToRetrain;
    private int numberOfEventsReceived;
    private ArrayList<DataPoint> dataPointsArray = new ArrayList<>();

    private boolean needToCheckTrainNow = false;
    private boolean modelTrained = false;
    private boolean initialTrained = false;
    private boolean decayRateGiven = false;
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
                int coordinateStartIndex;
                if (decayRateGiven) {
                    coordinateStartIndex = 4;
                } else {
                    coordinateStartIndex = 3;
                }
                for (int i=coordinateStartIndex; i<coordinateStartIndex+dimensionality; i++) {
                    Object content = attributeExpressionExecutors[i].execute(streamEvent);
                    if (content instanceof Double) {
                        coordinateValues[i-coordinateStartIndex] = (Double) content;
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
                if (!initialTrained) {
                    if (needToCheckTrainNow) {
                        boolean trainNow = (Boolean) attributeExpressionExecutors[2].execute(streamEvent);
                        if (trainNow) {
                            clusterer.cluster(dataPointsArray);
                            dataPointsArray.clear();
                            initialTrained = true;
                            modelTrained = true;
                        }
                    } else if (numberOfEventsToRetrain > 0) {
                        if (numberOfEventsReceived % numberOfEventsToRetrain == 0) {
                            clusterer.cluster(dataPointsArray);
                            dataPointsArray.clear();
                            initialTrained = true;
                            modelTrained = true;
                        }
                    }
                } else {
                    if (needToCheckTrainNow) {
                        boolean trainNow = (Boolean) attributeExpressionExecutors[2].execute(streamEvent);
                        if (trainNow) {
                            clusterer.updateCluster(dataPointsArray, decayRate);
                            dataPointsArray.clear();
                            modelTrained = true;
                        }
                    } else if (numberOfEventsToRetrain > 0) {
                        if (numberOfEventsReceived % numberOfEventsToRetrain == 0) {
                            clusterer.updateCluster(dataPointsArray, decayRate);
                            dataPointsArray.clear();
                            modelTrained = true;
                        }
                    }
                }

                if (modelTrained) {
                    Object[] outputData;
                    outputData = clusterer.getAssociatedCentroidInfo(currentDataPoint);
                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                } else {
                    streamEventChunk.remove();
                }
            } // should we need to handle RESET and EXPIRED?
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        //expressionExecutors[0] --> k
        if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("k has to be a constant.");
        }
        Object zerothContent = attributeExpressionExecutors[0].execute(null);
        if (zerothContent instanceof Integer) {
            k = (Integer) zerothContent;
        } else {
            throw new SiddhiAppValidationException("k should be of type int but found " +
                    attributeExpressionExecutors[0].getReturnType());
        }

        //expressionExecutors[1] --> maxIterations
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Maximum iterations has to be a constant.");
        }
        Object firstContent = attributeExpressionExecutors[1].execute(null);
        if (firstContent instanceof Integer) {
            maxIterations = (Integer) firstContent;
        } else {
            throw new SiddhiAppValidationException("Maximum iterations should be of type int but found " +
                    attributeExpressionExecutors[1].getReturnType());
        }

        //expressionExecutors[2] --> trainModel or numberOfEventsToRetrain
        if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
            needToCheckTrainNow = true;
        } else if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            needToCheckTrainNow = false;
            Object secondContent = attributeExpressionExecutors[2].execute(null);
            if (secondContent instanceof Integer) {
                numberOfEventsToRetrain = (Integer) secondContent;
            } else {
                throw new SiddhiAppValidationException("Number of events to trigger retraining" +
                        "should be of type int but found " +attributeExpressionExecutors[2].getReturnType());
            }
        } else {
            throw new SiddhiAppValidationException("The 4th parameter should either be boolean or int but found" +
                    attributeExpressionExecutors[2].getReturnType());
        }

        //expressionExecutors[3] --> decayRate or first dim of datapoint
        if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.FLOAT) {
            Object thirdContent = attributeExpressionExecutors[3].execute(null);
            if (thirdContent instanceof Float) {
                System.out.println("decay rate specified");
                decayRateGiven = true;
                decayRate = (Float) thirdContent;
                dimensionality = attributeExpressionExecutors.length - 4;
            }
        } else if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.DOUBLE) {
            System.out.println("decay rate not specified");
            decayRateGiven = false;
            decayRate = 0.001f; // default value for online approach. should tune
            dimensionality = attributeExpressionExecutors.length - 3;
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

