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
public class KMeansClusterer {
    private static final Logger logger = Logger.getLogger(KMeansClusterer.class.getName());

    public static void train(LinkedList<DataPoint> dataPointsArray, int numberOfEventsToRetrain, double decayRate,
                      ExecutorService executorService, KMeansModel model, int numberOfClusters,
                      int maximumIterations, int dimensionality) {
        if ((!model.isTrained())) {
            cluster(dataPointsArray, model, numberOfClusters, maximumIterations, dimensionality);
            dataPointsArray.clear();
            model.setTrained();
        } else {
            periodicTraining(numberOfEventsToRetrain, decayRate, executorService, dataPointsArray, model,
                    numberOfClusters, maximumIterations, dimensionality);
        }
    }

    private static void periodicTraining(int numberOfEventsToRetrain, double decayRate, ExecutorService executorService,
                                  LinkedList<DataPoint> dataPointsArray, KMeansModel model, int numberOfClusters,
                                  int maximumIterations, int dimensionality) {
        int minBatchSizeToTriggerSeparateThread = 20; //TODO: test and tune to optimum value
        if (numberOfEventsToRetrain < minBatchSizeToTriggerSeparateThread) {
            if (logger.isDebugEnabled()) {
                logger.debug("Traditional training");
            }
            updateCluster(dataPointsArray, decayRate, model, numberOfClusters, maximumIterations, dimensionality);
            dataPointsArray.clear();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Seperate thread training");
            }
            Trainer trainer = new Trainer(dataPointsArray, decayRate, model, numberOfClusters, maximumIterations,
                    dimensionality);
            Future f = executorService.submit(trainer);
        }
    }


    /**
     * Perform clustering
     */
    private static void cluster(List<DataPoint> dataPointsArray, KMeansModel model, int numberOfClusters,
                        int maximumIterations, int dimensionality) {
        if (logger.isDebugEnabled()) {
            logger.debug("initial Clustering");
        }
        buildModel(dataPointsArray, model, numberOfClusters);

        int iter = 0;
        if (dataPointsArray.size() != 0 && (model.size() == numberOfClusters)) {
            boolean centroidShifted;
            while (iter < maximumIterations) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Current model : \n" + model.getModelInfo() + "\nclustering iteration : " + iter);
                }
                assignToCluster(dataPointsArray, model);
                if (logger.isDebugEnabled()) {
                    logger.debug("Current model : \n" + model.getModelInfo());
                }
                List<Cluster> newClusterList = calculateNewClusters(model, dimensionality);

                centroidShifted = !model.getClusterList().equals(newClusterList);
                if (logger.isDebugEnabled()) {
                    logger.debug("previous model : " + printClusterList(model.getClusterList()) + "\nnew model : " +
                            printClusterList(newClusterList) + "\ncentroid shifted?" + centroidShifted);
                }
                if (!centroidShifted) {
                    break;
                }
                model.setClusterList(newClusterList);
                iter++;
            }
        }
    }

    private static String printClusterList(List<Cluster> clusterList) {
        StringBuilder s = new StringBuilder();
        for (Cluster c: clusterList) {
            s.append(Arrays.toString(c.getCentroid().getCoordinates()));
        }
        return s.toString();
    }

    private static void buildModel(List<DataPoint> dataPointsArray, KMeansModel model, int numberOfClusters) {
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
     */
    static void updateCluster(List<DataPoint> dataPointsArray, double decayRate, KMeansModel model,
                              int numberOfClusters, int maximumIterations, int dimensionality) {
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
                buildModel(dataPointsArray, model, numberOfClusters);
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

                    assignToCluster(dataPointsArray, model);
                    List<Cluster> newClusterList = calculateNewClusters(model, dimensionality);
                    centroidShifted = !intermediateClusterList.equals(newClusterList);
                    if (logger.isDebugEnabled()) {
                        s = new StringBuilder();
                        for (DataPoint c : dataPointsArray) {
                            s.append(Arrays.toString(c.getCoordinates()));
                        }
                        logger.debug("current iteration : " + iter + "\ndata points array\n" + s.toString() +
                                "\nCluster list : \n" + printClusterList(intermediateClusterList) +
                                "\nnew cluster list \n" + printClusterList(newClusterList) + "\nCentroid shifted? = "
                                + centroidShifted + "\n");
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
                    logger.debug("old cluster list :\n" + printClusterList(oldClusterList));
                }
                for (int i = 0; i < numberOfClusters; i++) {
                    if (model.getClusterList().get(i).getDataPointsInCluster().size() != 0) {
                        double[] weightedCoordinates = new double[dimensionality];
                        double[] oldCoordinates = oldClusterList.get(i).getCentroid().getCoordinates();
                        double[] newCoordinates = intermediateClusterList.get(i).getCentroid().getCoordinates();
                        for (int j = 0; j < dimensionality; j++) {
                            weightedCoordinates[j] = Math.round(((1 - decayRate) * oldCoordinates[j] + decayRate *
                                    newCoordinates[j]) * 10000.0) / 10000.0;
                        }
                        intermediateClusterList.get(i).getCentroid().setCoordinates(weightedCoordinates);
                    } else {
                        intermediateClusterList.get(i).getCentroid().setCoordinates(
                                oldClusterList.get(i).getCentroid().getCoordinates());
                    }
                }
                model.setClusterList(intermediateClusterList);
                if (logger.isDebugEnabled()) {
                    logger.debug("weighted centroid list\n" + printClusterList(model.getClusterList()));
                }
            }
        }
    }

    /**
     * finds the nearest centroid to each data point in the input array
     */
    private static void assignToCluster(List<DataPoint> dataPointsArray, KMeansModel model) {
        logger.debug("Running function assignToCluster");
        model.clearClusterMembers();
        for (DataPoint currentDataPoint : dataPointsArray) {
            Cluster associatedCluster = findAssociatedCluster(currentDataPoint, model);
            logger.debug("Associated cluster of " + Arrays.toString(currentDataPoint.getCoordinates()) + " is " +
             Arrays.toString(associatedCluster.getCentroid().getCoordinates()));
            associatedCluster.addToCluster(currentDataPoint);
        }
    }

    /**
     * finds the nearest centroid to a given DataPoint
     * @return centroid - the nearest centroid to the input DataPoint
     */
    private static Cluster findAssociatedCluster(DataPoint currentDatapoint, KMeansModel model) {
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
     * @return an Object[] array as mentioned above
     */
    public static Object[] getAssociatedCentroidInfo(DataPoint currentDatapoint, KMeansModel model) {
        Cluster associatedCluster = findAssociatedCluster(currentDatapoint, model);
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
    private static List<Cluster> calculateNewClusters(KMeansModel model, int dimensionality) {
        List<Cluster> newClusterList = new LinkedList<>();

        for (Cluster c: model.getClusterList()) {
            double[] total;
            total = new double[dimensionality];
            for (DataPoint d: c.getDataPointsInCluster()) {
                double[] coordinatesOfd = d.getCoordinates();
                for (int i = 0; i < dimensionality; i++) {
                    total[i] += coordinatesOfd[i];
                }
            }
            int numberOfMembers = c.getDataPointsInCluster().size();
            for (int i = 0; i < dimensionality; i++) {
                total[i] = Math.round((total[i] / numberOfMembers) * 10000.0) / 10000.0;
            }

            DataPoint d1 = new DataPoint();
            d1.setCoordinates(total);
            Cluster c1 = new Cluster(d1);
            newClusterList.add(c1);
        }
        return newClusterList;
    }
}
