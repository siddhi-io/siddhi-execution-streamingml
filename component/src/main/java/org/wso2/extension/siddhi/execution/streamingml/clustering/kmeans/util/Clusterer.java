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

package org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.streamingml.util.MathUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * class containing all mathematical logic needed to perform kmeans
 */
public class Clusterer {
    private int numberOfClusters;
    private int maximumIterations;
    private boolean initialTrained = false;
    private boolean modelTrained = false;
    private String siddhiAppName;
    private KMeansModel model;
    private int dimensionality;
    private static final Logger logger = Logger.getLogger(Clusterer.class.getName());

    /**
     * Initialize no. of cluster centers, no. of iterations
     */
    public Clusterer(int numberOfClusters, int maximumIterations, String modelName, String siddhiAppName,
                     int dimensionality) {
        model = KMeansModelHolder.getInstance().getKMeansModel(modelName);
        //logger.setLevel(Level.ALL);
        if (model == null) {
            model = new KMeansModel();
            KMeansModelHolder.getInstance().addKMeansModel(modelName, model);
            if (logger.isDebugEnabled()) {
                logger.debug("New model is created with name " + modelName);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Reusing an existing model with name " + modelName);
            }
            if (model.size() == numberOfClusters) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Existing model " + modelName + " is trained");
                }
                initialTrained = true;
                modelTrained = true;
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Existing model " + modelName + " is not trained");
                }
            }
        }
        this.numberOfClusters = numberOfClusters;
        this.maximumIterations = maximumIterations;
        this.siddhiAppName = siddhiAppName;
        this.dimensionality = dimensionality;
    }

    public boolean isInitialTrained() {
        return initialTrained;
    }

    public boolean isModelTrained() {
        return modelTrained;
    }

    public void train(LinkedList<DataPoint> dataPointsArray, int numberOfEventsToRetrain, double decayRate,
                      ExecutorService executorService) {
        if ((!initialTrained)) {
            cluster(dataPointsArray);
            dataPointsArray.clear();
            initialTrained = true;
            modelTrained = true;
        } else {
            periodicTraining(numberOfEventsToRetrain, decayRate, executorService, dataPointsArray);
        }
    }

    private void periodicTraining(int numberOfEventsToRetrain, double decayRate, ExecutorService executorService,
                                  LinkedList<DataPoint> dataPointsArray) {
        int minBatchSizeToTriggerSeparateThread = 100; //TODO: test and tune to optimum value
        if (numberOfEventsToRetrain < minBatchSizeToTriggerSeparateThread) {
            if (logger.isDebugEnabled()) {
                logger.debug("Traditional training in " + siddhiAppName);
            }
            updateCluster(dataPointsArray, decayRate);
            dataPointsArray.clear();
            modelTrained = true;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Seperate thread training in " + siddhiAppName);
            }
            Trainer trainer = new Trainer(this, dataPointsArray, decayRate);
            Future f = executorService.submit(trainer);
        }
    }

    public void setModel(KMeansModel m) {
        model = m;
    }

    /**
     * Perform clustering
     */
    public void cluster(List<DataPoint> dataPointsArray) {
        if (logger.isDebugEnabled()) {
            logger.debug("initial Clustering");
        }
        buildModel(dataPointsArray);

        int iter = 0;
        if (dataPointsArray.size() != 0 && (model.size() == numberOfClusters)) {
            boolean centroidShifted = false;
            while (iter < maximumIterations) {
                logger.debug("Current model : \n" + model.getModelInfo());
                logger.debug("clustering iteration : " + iter);
                assignToCluster(dataPointsArray);
                logger.debug("Current model : \n" + model.getModelInfo());
                List<Cluster> newClusterList = calculateNewClusters();

                logger.debug("previous model : " + printClusterList(model.getClusterList()));
                logger.debug("new model : " + printClusterList(newClusterList));
                centroidShifted = !model.getClusterList().equals(newClusterList);
                logger.debug("centroid shifted?" + centroidShifted);
                if (!centroidShifted) {
                    break;
                }
                model.setClusterList(newClusterList);
                iter++;
            }
        }
    }

    public String printClusterList(List<Cluster> clusterList) {
        StringBuilder s = new StringBuilder();
        for (Cluster c: clusterList) {
            s.append(Arrays.toString(c.getCentroid().getCoordinates()));
        }
        return s.toString();
    }

    public void buildModel(List<DataPoint> dataPointsArray) {
        int distinctCount = model.size();
        for (DataPoint currentDataPoint : dataPointsArray) {
            if (distinctCount >= numberOfClusters) {
                break;
            }
            DataPoint coordinatesOfCurrentDataPoint = new DataPoint();
            coordinatesOfCurrentDataPoint.setCoordinates(currentDataPoint.getCoordinates());
            if (!model.contains(coordinatesOfCurrentDataPoint)) {
                model.add(coordinatesOfCurrentDataPoint);
                distinctCount++;
            }
        }
    }

    /**
     * After the first clustering this method can be used to incrementally update centroidList
     * in real time. This method takes in the new set of datapoints and decayRate as inputs
     * and calculates the centroids of the new set. Then new centroids are calculated using
     * newAvg = oldAvg + decayRate * batchAvg
     *
     * @param dataPointsArray
     * @param decayRate       should be in [0,1]
     */
    public void updateCluster(List<DataPoint> dataPointsArray, double decayRate) {
        if (logger.isDebugEnabled()) {
            logger.debug("Updating cluster");
            logger.debug("model at the start of this update : ");
            logger.debug(model.getModelInfo());
        }
        StringBuilder s;
        List<Cluster> intermediateClusterList = new LinkedList<>();

        int iter = 0;
        if (dataPointsArray.size() != 0) {
            //when number of elements in centroid list is less than numberOfClusters
            if (model.size() < numberOfClusters) {
                buildModel(dataPointsArray);
            }
            if (model.size() == numberOfClusters) {
                ArrayList<Cluster> oldClusterList = new ArrayList<>(numberOfClusters);
                for (int i = 0; i < numberOfClusters; i++) {
                    DataPoint d = new DataPoint();
                    DataPoint d1 = new DataPoint();
                    d.setCoordinates(model.getCoordinatesOfCentroidOfCluster(i));
                    d1.setCoordinates(model.getCoordinatesOfCentroidOfCluster(i));
                    Cluster c = new Cluster(d);;
                    Cluster c1 = new Cluster(d1);
                    oldClusterList.add(c);
                    intermediateClusterList.add(c1);
                }
                boolean centroidShifted = false;
                while (iter < maximumIterations) {
                    assignToCluster(dataPointsArray);
                    s = new StringBuilder();
                    for (DataPoint c : dataPointsArray) {
                        s.append(Arrays.toString(c.getCoordinates()));

                    }
                    List<Cluster> newClusterList = calculateNewClusters();
                    centroidShifted = !intermediateClusterList.equals(newClusterList);
                    if (logger.isDebugEnabled()) {
                        logger.debug("current iteration : " + iter);
                        logger.debug("data points array");
                        logger.debug(s.toString());
                        logger.debug("Cluster list : ");
                        logger.debug(printClusterList(intermediateClusterList));
                        logger.debug("new cluster list ");
                        logger.debug(printClusterList(newClusterList));
                        logger.debug("Centroid shifted? = " + centroidShifted + "\n");
                    }
                    if (!centroidShifted) {
                        break;
                    }
                    model.setClusterList(newClusterList);
                    for (int i = 0; i < numberOfClusters; i++) {
                        Cluster b = newClusterList.get(i);
                        intermediateClusterList.get(i).getCentroid().setCoordinates(b.getCentroid().getCoordinates());
                    }
                    iter++;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("old cluster list :");
                    logger.debug(printClusterList(oldClusterList));
                }
                for (int i = 0; i < numberOfClusters; i++) {
                    if (model.getClusterList().get(i).getDataPointsInCluster().size() != 0) {
                        double[] weightedCoordinates = new double[dimensionality];
                        double[] oldCoordinates = oldClusterList.get(i).getCentroid().getCoordinates();
                        double[] newCoordinates = intermediateClusterList.get(i).getCentroid().getCoordinates();
                        Arrays.setAll(weightedCoordinates, j -> Math.round(((1 - decayRate) * oldCoordinates[j] +
                                decayRate * newCoordinates[j]) * 10000.0) / 10000.0);
                        intermediateClusterList.get(i).getCentroid().setCoordinates(weightedCoordinates);
                    } else {
                        intermediateClusterList.get(i).getCentroid().setCoordinates(
                                oldClusterList.get(i).getCentroid().getCoordinates());
                    }
                }
                model.setClusterList(intermediateClusterList);
                if (logger.isDebugEnabled()) {
                    logger.debug("weighted centroid list");
                    logger.debug(printClusterList(model.getClusterList()));
                }
            }
        }
    }

    /**
     * finds the nearest centroid to each data point in the input array
     * @param dataPointsArray arraylist containing datapoints for which we need to assign centroids
     */
    private void assignToCluster(List<DataPoint> dataPointsArray) {
        logger.debug("Running function assignToCluster");
        model.clearClusterMembers();
        for (DataPoint currentDataPoint : dataPointsArray) {
            Cluster associatedCluster = findAssociatedCluster(currentDataPoint);
            logger.debug("Associated cluster of " + Arrays.toString(currentDataPoint.getCoordinates()) + " is " +
             Arrays.toString(associatedCluster.getCentroid().getCoordinates()));
            associatedCluster.addToCluster(currentDataPoint);
        }
    }

    /**
     * finds the nearest centroid to a given DataPoint
     *
     * @param currentDatapoint input DataPoint to which we need to find nearest centroid
     * @return centroid - the nearest centroid to the input DataPoint
     */
    private Cluster findAssociatedCluster(DataPoint currentDatapoint) {
        double minDistance = MathUtil.euclideanDistance(model.getCoordinatesOfCentroidOfCluster(0),
                currentDatapoint.getCoordinates());
        Cluster associatedCluster = model.getClusterList().get(0);
        for (int i = 0; i < model.size(); i++) {
            Cluster cluster = model.getClusterList().get(i);
            double dist = MathUtil.euclideanDistance(cluster.getCentroid().getCoordinates(),
                    currentDatapoint.getCoordinates());
            if (dist < minDistance) {
                minDistance = dist;
                associatedCluster = cluster;
            }
        }
        return associatedCluster;
    }

    /**
     * similar to findAssociatedCluster method but return an Object[] array with the distance
     * to closest centroid and the coordinates of the closest centroid
     *
     * @param currentDatapoint the input dataPoint for which the closest centroid needs to be found
     * @return an Object[] array as mentioned above
     */
    public Object[] getAssociatedCentroidInfo(DataPoint currentDatapoint) {
        Cluster associatedCluster = findAssociatedCluster(currentDatapoint);
        double minDistance = MathUtil.euclideanDistance(currentDatapoint.getCoordinates(),
                associatedCluster.getCentroid().getCoordinates());
        List<Double> associatedCentroidInfoList = new ArrayList<Double>();
        associatedCentroidInfoList.add(minDistance);

        for (double x : associatedCluster.getCentroid().getCoordinates()) {
            associatedCentroidInfoList.add(x);
        }

        Object[] associatedCentroidInfo = new Object[associatedCentroidInfoList.size()];
        associatedCentroidInfoList.toArray(associatedCentroidInfo);
        return associatedCentroidInfo;
    }

    /**
     * after assigning data points to closest centroids this method calculates new centroids using
     * the assigned points
     *
     * @return returns an array list of coordinate objects each representing a centroid
     */
    private List<Cluster> calculateNewClusters() {
        List<Cluster> newClusterList = new LinkedList<>();

        for (Cluster c: model.getClusterList()) {
            double[] total;
            total = new double[dimensionality];
            for (DataPoint d: c.getDataPointsInCluster()) {
                Arrays.setAll(total, i -> total[i] + d.getCoordinates()[i]);
            }
            Arrays.setAll(total, i -> Math.round((total[i] / c.getDataPointsInCluster().size()) * 10000.0) / 10000.0);

            DataPoint d1 = new DataPoint();
            d1.setCoordinates(total);
            Cluster c1 = new Cluster(d1);
            newClusterList.add(c1);
        }
        return newClusterList;
    }
}
