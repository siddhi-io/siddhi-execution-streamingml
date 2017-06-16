package org.wso2.extension.siddhi.execution.ml.kmeans;


import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
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
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

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
    private int clusters;
    private int iterations;

    private int eventsToTrain;
    private int count;
    private ArrayList<ClusterObject> clusterDataList = new ArrayList<>();

    private boolean trainModel;
    private boolean modelTrained;
    private Clusterer clusterer;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                count++;
                double value = (Double) attributeExpressionExecutors[0].execute(streamEvent);
                ClusterObject clusterData = new ClusterObject();
                clusterData.setValue(value);
                clusterDataList.add(clusterData);
                if (trainModel) {
                    boolean trainNow = (Boolean) attributeExpressionExecutors[3].execute(streamEvent);
                    if (trainNow) {
                        clusterer.cluster(clusterDataList);
                        modelTrained = true;
                    }
                }else if (eventsToTrain > 0) {
                    if (count % eventsToTrain == 0) {
                        clusterer.cluster(clusterDataList);
                        modelTrained = true;
                    }
                }
                if (modelTrained) {
                    Object[] outputData = null;
                    outputData = clusterer.getCenter(value);
                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                } else {
                    streamEventChunk.remove();
                }
            } else if (streamEvent.getType() == ComplexEvent.Type.RESET) {
                clusterDataList.clear();
                streamEventChunk.remove();
            } else if (streamEvent.getType() == ComplexEvent.Type.EXPIRED) {
                double value = (Double) attributeExpressionExecutors[0].execute(streamEvent);
                ClusterObject clusterData = new ClusterObject();
                clusterData.setValue(value);
                clusterDataList.remove(clusterData);
                streamEventChunk.remove();
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, ExecutionPlanContext executionPlanContext) {

        if (!(attributeExpressionExecutors.length == 4)) {
            throw new ExecutionPlanValidationException(
                    "Clustering function has to have exactly 4 parameters, currently "
                            + attributeExpressionExecutors.length + " parameters provided.");

        }

        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Cluster centers has to be a constant.");
        }
        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Iterations centers has to be a constant.");
        }

        Object clustersObject = attributeExpressionExecutors[1].execute(null);
        if (clustersObject instanceof Integer) {
            clusters = (Integer) clustersObject;
        } else {
            throw new ExecutionPlanValidationException("Cluster centers should be of type int. But found "
                    + attributeExpressionExecutors[1].getReturnType());
        }

        Object iterationsObject = attributeExpressionExecutors[2].execute(null);
        if (iterationsObject instanceof Integer) {
            iterations = (Integer) iterationsObject;

        } else {
            throw new ExecutionPlanValidationException("Iterations should be of type int. But found "
                    + attributeExpressionExecutors[2].getReturnType());
        }

        if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.BOOL) {
            trainModel = true;

        } else if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
            Object trainingOptionObject = attributeExpressionExecutors[3].execute(null);
            if (trainingOptionObject instanceof Integer) {
                eventsToTrain = (Integer) trainingOptionObject;

            } else {
                throw new ExecutionPlanValidationException("No. of events to train should be of type integer. " +
                        "But found " + attributeExpressionExecutors[3].getReturnType());
            }
        } else {
            throw new ExecutionPlanValidationException("The 4th parameter can be either events.to.train or train. It" +
                    "should either be of type boolean or int but found"
                    + attributeExpressionExecutors[3].getReturnType());
        }

        clusterer = new Clusterer(clusters, iterations);


        List<Attribute> attributeList = new ArrayList<>(3);
        attributeList.add(new Attribute("matchedClusterCentroid", Attribute.Type.DOUBLE));
        attributeList.add(new Attribute("matchedClusterIndex", Attribute.Type.INT));
        attributeList.add(new Attribute("distanceToCenter", Attribute.Type.DOUBLE));
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
        map.put("data", clusterDataList);
        map.put("modelTrained", modelTrained);
        map.put("count", count);
        return map;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        clusterDataList = (ArrayList<ClusterObject>) map.get("data");
        modelTrained = (Boolean) map.get("modelTrained");
        count = (Integer) map.get("count");
    }


}
