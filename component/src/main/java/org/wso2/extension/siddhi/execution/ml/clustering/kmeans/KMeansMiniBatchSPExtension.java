package org.wso2.extension.siddhi.execution.ml.clustering.kmeans;

import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.Clusterer;
import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.DataPoint;
import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.KMeansModel;
import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.KMeansModelHolder;
import org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util.Trainer;
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
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;


@Extension(
        name = "KMeansMiniBatch",
        namespace = "streamingML",
        description = "Performs K-Means clustering on a streaming data set. Data points can be of any dimension and the dimensionality is calculated from number of parameters. " +
                "All data points to be processed by an instance of class Clusterer should be of the same dimensionality. The Euclidean distance is taken as the distance metric. " +
                "The algorithm resembles mini-batch K-Means. (refer Web-Scale K-Means Clustering by D.Sculley, Google, Inc.). " +
                "For example: #streamingML:KMeansMiniBatch(dimensionality, k, maxIterations, train, x1, x2, .... , xd)" +
                "Model is trained for every specified number of events . The training process initializes the first k distinct events in the window as" +
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
                        description = "The assumed number of natural clusters (k) in the data set.",
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
                        type = DataType.INT
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
                        syntax = "from InputStream#kmeans:cluster(dimensionality, k, maxIterations, numberOfEventsToRetrain, decayRate, coordinateValue1, coordinateValue2)\"\n" +
                                "select coordinateValue1, coordinateValue2, euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, closestCentroidCoordinate2\"\n" +
                                "insert into OutputStream",
                        description = "dimensionality =2, k=2, numberOfEventsToRetrain = 5, maxIterations=10. This will cluster the collected data points within the window for every 5 events" +
                                "and give output after the first 5 events"
                ),
        }
)
public class KMeansMiniBatchSPExtension extends StreamProcessor {
    private int k;
    private int maxIterations;
    private float decayRate;
    private String modelName;


    private int numberOfEventsToRetrain;
    private int numberOfEventsReceived;
    private int coordinateStartIndex;
    private ArrayList<DataPoint> dataPointsArray = new ArrayList<>();

    private boolean modelTrained = false;
    private boolean initialTrained = false;
    private Clusterer clusterer;
    private int dimensionality;
    private double[] coordinateValues;
    private int minBatchSizeToTriggerSeparateThread = 10;
    private ExecutorService executorService;
    private final static Logger logger = Logger.getLogger(KMeansMiniBatchSPExtension.class.getName());

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();

            if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                numberOfEventsReceived++;
                coordinateValues = new double[dimensionality];


                //validating and getting coordinate values
                for (int i=coordinateStartIndex; i<coordinateStartIndex+dimensionality; i++) {
                    //Object content = ;

                    try {
                        Number content = (Number) attributeExpressionExecutors[i].execute(streamEvent);
                        coordinateValues[i-coordinateStartIndex] = content.doubleValue();
                    } catch (ClassCastException e) {
                        throw new SiddhiAppValidationException("coordinate values should be int/float/double/long but found " +
                                attributeExpressionExecutors[i].execute(streamEvent).getClass());
                    }
                }

                //creating a dataPoint with the received coordinate values
                DataPoint currentDataPoint = new DataPoint(dimensionality);
                currentDataPoint.setCoordinates(coordinateValues);
                dataPointsArray.add(currentDataPoint);

                //handling the training
                if ((!initialTrained) && (numberOfEventsToRetrain > 0) && (numberOfEventsReceived % numberOfEventsToRetrain == 0)) {
                    clusterer.cluster(dataPointsArray);
                    dataPointsArray.clear();
                    initialTrained = true;
                    modelTrained = true;
                } else if (numberOfEventsToRetrain > 0 && (numberOfEventsReceived % numberOfEventsToRetrain == 0)){
                    periodicTraining();

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

    private void periodicTraining() {
        if (numberOfEventsToRetrain< minBatchSizeToTriggerSeparateThread) {
            logger.config("Traditional training");
            clusterer.updateCluster(dataPointsArray, decayRate);
            dataPointsArray.clear();
            modelTrained = true;
        } else {
            logger.config("Seperate thread training");
            Trainer trainer = new Trainer(clusterer, dataPointsArray, decayRate);
            executorService.submit(trainer);
            /*Thread t = new Thread(trainer);
            t.start();*/
        }
    }



    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        logger.setLevel(Level.ALL);
        //expressionExecutors[0] --> modelName
        if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("modelName has to be a constant.");
        }
        Object zerothContent = attributeExpressionExecutors[0].execute(null);
        if (zerothContent instanceof String) {
            modelName = (String) zerothContent;
        } else {
            throw new SiddhiAppValidationException("modelName should be of type String but found " +
                    attributeExpressionExecutors[0].getReturnType());
        }

        //new
        //expressionExecutors[1] --> decayRate or k
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("4th parameter can be decayRate/k. Both has to be a constant.");
        }
        Object firstContent = attributeExpressionExecutors[1].execute(null);
        if (firstContent instanceof Float) {
            logger.info("Decay rate is specified.");
            decayRate = (Float) firstContent;
            coordinateStartIndex = 5;

            //expressionExecutors[2] --> k
            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("k has to be a constant.");
            }
            Object secondContent = attributeExpressionExecutors[2].execute(null);
            if (secondContent instanceof Integer) {
                k = (Integer) secondContent;
            } else {
                throw new SiddhiAppValidationException("k should be of type int but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

        } else if (firstContent instanceof Integer){
            logger.info("Decay rate is not specified. using default");
            decayRate = 0.01f;
            coordinateStartIndex = 4;
            k = (Integer) firstContent;
        }else {
            throw new SiddhiAppValidationException("decayRate/k should be of type float/int but found " +
                    attributeExpressionExecutors[1].getReturnType());
        }
        //new end

        //expressionExecutors[coordinateStartIndex-2] --> maxIterations
        if (!(attributeExpressionExecutors[coordinateStartIndex-2] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Maximum iterations has to be a constant.");
        }
        Object Content_2 = attributeExpressionExecutors[coordinateStartIndex-2].execute(null);
        if (Content_2 instanceof Integer) {
            maxIterations = (Integer) Content_2;
        } else {
            throw new SiddhiAppValidationException("Maximum iterations should be of type int but found " +
                    attributeExpressionExecutors[coordinateStartIndex-2].getReturnType());
        }

        //expressionExecutors[coordinateStartIndex-1] --> numberOfEventsToRetrain
        if (!(attributeExpressionExecutors[coordinateStartIndex-1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("numberOfEventsToRetrain has to be a constant.");
        }
        Object Content_1 = attributeExpressionExecutors[coordinateStartIndex-1].execute(null);
        if (Content_1 instanceof Integer) {
            numberOfEventsToRetrain = (Integer) Content_1;
        } else {
            throw new SiddhiAppValidationException("numberOfEventsToRetrain should be of type int but found " +
                    attributeExpressionExecutors[coordinateStartIndex-1].getReturnType());
        }

        dimensionality = attributeExpressionExecutors.length - coordinateStartIndex;

        String siddhiAppName = siddhiAppContext.getName();
        modelName = modelName+"."+siddhiAppName;
        logger.info(modelName);
        clusterer = new Clusterer(k,maxIterations, modelName);

        executorService = siddhiAppContext.getExecutorService();
        logger.setLevel(Level.ALL);

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
        map.put("model", KMeansModelHolder.getInstance().getClonedKMeansModelMap());
        return map;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        dataPointsArray = (ArrayList<DataPoint>) map.get("data");
        modelTrained = (Boolean) map.get("modelTrained");
        numberOfEventsReceived = (Integer) map.get("numberOfEventsReceived");
        KMeansModel model1 = (KMeansModel) map.get("model");
        clusterer.setModel(model1);
    }


}

