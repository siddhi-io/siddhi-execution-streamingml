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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.streamingml.util.Coordinates;
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
    private int[] countOfDataPointsAssignedToEachCentroid;
    private static final Logger logger = Logger.getLogger(Clusterer.class.getName());

    /**
     * Initialize no. of cluster centers, no. of iterations
     */
    public Clusterer(int numberOfClusters, int maximumIterations, String modelName, String siddhiAppName,
                     int dimensionality) {
        logger.setLevel(Level.ALL);
        model = KMeansModelHolder.getInstance().getKMeansModel(modelName);
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
        this.countOfDataPointsAssignedToEachCentroid = new int[numberOfClusters];
    }

    public boolean isInitialTrained() {
        return initialTrained;
    }

    public boolean isModelTrained() {
        return modelTrained;
    }

    public void train(LinkedList<DataPoint> dataPointsArray, int numberOfEventsToRetrain, float decayRate,
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


    private void periodicTraining(int numberOfEventsToRetrain, float decayRate, ExecutorService executorService,
                                  LinkedList<DataPoint> dataPointsArray) {
        int minBatchSizeToTriggerSeparateThread = 10; //TODO: test and tune to optimum value
        if (numberOfEventsToRetrain < minBatchSizeToTriggerSeparateThread) {
            logger.debug("Traditional training in " + siddhiAppName);
            updateCluster(dataPointsArray, decayRate);
            dataPointsArray.clear();
            modelTrained = true;
        } else {
            logger.debug("Seperate thread training in " + siddhiAppName);
            Trainer trainer = new Trainer(this, dataPointsArray, decayRate);
            Future f = executorService.submit(trainer);
        }
    }

    public void setModel(KMeansModel m) {
        model = m;
//        initialTrained = true;
//        modelTrained = true;
    }

    /**
     * Perform clustering
     */
    public void cluster(List<DataPoint> dataPointsArray) {
        logger.info("initial Clustering");
        buildModel(dataPointsArray);

        int iter = 0;
        if (dataPointsArray.size() != 0 && (model.size() == numberOfClusters)) {
            boolean centroidShifted = false;
            while (iter < maximumIterations) {
                assignToCluster(dataPointsArray);
                List<Coordinates> newCentroidList = calculateNewCentroids(dataPointsArray);

                centroidShifted = !model.getCentroidList().equals(newCentroidList);
                if (!centroidShifted) {
                    break;
                }
                for (int i = 0; i < numberOfClusters; i++) {
                    model.update(i, newCentroidList.get(i).getCoordinates());
                }
                iter++;
            }
        }
    }

    public void buildModel(List<DataPoint> dataPointsArray) {
        int distinctCount = model.size();
        for (DataPoint currentDataPoint : dataPointsArray) {
            if (distinctCount >= numberOfClusters) {
                break;
            }
            Coordinates coordinatesOfCurrentDataPoint = new Coordinates();
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
    public void updateCluster(List<DataPoint> dataPointsArray, float decayRate) {
        logger.info("Updating cluster");
        StringBuilder s;

        List<Coordinates> intermediateCentroidList = new LinkedList<>();


        int iter = 0;
        if (dataPointsArray.size() != 0) {

            //when number of elements in centroid list is less than numberOfClusters
            if (model.size() < numberOfClusters) {
                buildModel(dataPointsArray);
            }

            if (model.size() == numberOfClusters) {
                ArrayList<Coordinates> oldCentroidList = new ArrayList<>(numberOfClusters);
                for (int i = 0; i < numberOfClusters; i++) {
                    Coordinates c = new Coordinates();
                    Coordinates c1 = new Coordinates();
                    c.setCoordinates(model.getCoordinatesOfCentroid(i));
                    c1.setCoordinates(model.getCoordinatesOfCentroid(i));
                    oldCentroidList.add(c);
                    intermediateCentroidList.add(c1);
                }
                boolean centroidShifted = false;
                while (iter < maximumIterations) {
                    assignToCluster(dataPointsArray);
                    logger.info("data points array");
                    s = new StringBuilder();
                    for (Coordinates c : dataPointsArray) {
                        s.append(Arrays.toString(c.getCoordinates()));

                    }
                    logger.info(s.toString());
                    List<Coordinates> newCentroidList = calculateNewCentroids(dataPointsArray);

                    centroidShifted = !intermediateCentroidList.equals(newCentroidList);

                    logger.info("centroid list");
                    s = new StringBuilder();
                    for (Coordinates c : model.getCentroidList()) {
                        s.append(Arrays.toString(c.getCoordinates()));
                    }
                    logger.info(s.toString());
                    logger.info("new centroid list");
                    s = new StringBuilder();
                    for (Coordinates c : newCentroidList) {
                        s.append(Arrays.toString(c.getCoordinates()));
                    }
                    logger.info(s.toString());

                    logger.info("Centroid shifted? = " + centroidShifted);
                    if (!centroidShifted) {
                        break;
                    }
                    for (int i = 0; i < numberOfClusters; i++) {
                        intermediateCentroidList.get(i).setCoordinates(newCentroidList.get(i).getCoordinates());
                    }
                    iter++;
                }
                logger.info("old centroid list");
                s = new StringBuilder();
                for (Coordinates c : oldCentroidList) {
                    s.append(Arrays.toString(c.getCoordinates()));
                }
                logger.info(s.toString());
                for (int i = 0; i < numberOfClusters; i++) {
                    if (countOfDataPointsAssignedToEachCentroid[i] > 0) {
                        double[] weightedCoordinates = new double[dimensionality];
                        double[] oldCoordinates = oldCentroidList.get(i).getCoordinates();
                        double[] newCoordinates = intermediateCentroidList.get(i).getCoordinates();
                        Arrays.setAll(weightedCoordinates, j -> Math.round(((1 - decayRate) * oldCoordinates[j] +
                                decayRate * newCoordinates[j]) * 10000.0) / 10000.0);
                        logger.info("weighted" + Arrays.toString(weightedCoordinates));
                        intermediateCentroidList.get(i).setCoordinates(weightedCoordinates);
                        //model.update(i, weightedCoordinates);
                    } else {
                        intermediateCentroidList.get(i).setCoordinates(oldCentroidList.get(i).getCoordinates());
                        //model.update(i, );
                    }
                }
                model.setCentroidList(intermediateCentroidList);
                logger.info("weighted centroid list");
                s = new StringBuilder();
                for (Coordinates c : model.getCentroidList()) {
                    s.append(Arrays.toString(c.getCoordinates()));
                }
                logger.info(s.toString());
            }
        }
    }

    /**
     * finds the nearest centroid to each data point in the input array
     *
     * @param dataPointsArray arraylist containing datapoints for which we need to assign centroids
     */
    private void assignToCluster(List<DataPoint> dataPointsArray) {
        for (DataPoint currentDataPoint : dataPointsArray) {
            Coordinates associatedCentroid = findAssociatedCentroid(currentDataPoint);
            currentDataPoint.setAssociatedCentroid(associatedCentroid);
        }
    }

    /**
     * finds the nearest centroid to a given DataPoint
     *
     * @param currentDatapoint input DataPoint to which we need to find nearest centroid
     * @return centroid - the nearest centroid to the input DataPoint
     */
    private Coordinates findAssociatedCentroid(DataPoint currentDatapoint) {
        double minDistance = MathUtil.euclideanDistance(model.getCoordinatesOfCentroid(0),
                currentDatapoint.getCoordinates());
        Coordinates associatedCentroid = model.getCentroid(0);
        for (int i = 0; i < model.size(); i++) {
            Coordinates centroid = model.getCentroid(i);
            double dist = MathUtil.euclideanDistance(centroid.getCoordinates(), currentDatapoint.getCoordinates());
            if (dist < minDistance) {
                minDistance = dist;
                associatedCentroid = centroid;
            }
        }
        return associatedCentroid;
    }

    /**
     * similar to findAssociatedCentroid method but return an Object[] array with the distance
     * to closest centroid and the coordinates of the closest centroid
     *
     * @param currentDatapoint the input dataPoint for which the closest centroid needs to be found
     * @return an Object[] array as mentioned above
     */
    public Object[] getAssociatedCentroidInfo(DataPoint currentDatapoint) {

        Coordinates associatedCentroid = findAssociatedCentroid(currentDatapoint);
        double minDistance = MathUtil.euclideanDistance(currentDatapoint.getCoordinates(),
                associatedCentroid.getCoordinates());
        List<Double> associatedCentroidInfoList = new ArrayList<Double>();
        associatedCentroidInfoList.add(minDistance);
        for (double x : associatedCentroid.getCoordinates()) {
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
    private List<Coordinates> calculateNewCentroids(List<DataPoint> dataPointsArray) {
        ArrayList<double[]> total = new ArrayList<>();
        List<Coordinates> newCentroidList = new LinkedList<>();

        for (int i = 0; i < numberOfClusters; i++) {
            countOfDataPointsAssignedToEachCentroid[i] = 0;
            total.add(new double[dimensionality]);

            Coordinates c = new Coordinates();
            newCentroidList.add(c);
        }


        for (int y = 0; y < dataPointsArray.size(); y++) {
            Coordinates associatedCen = dataPointsArray.get(y).getAssociatedCentroid();
            int index = model.indexOf(associatedCen);
            int c = y;
            countOfDataPointsAssignedToEachCentroid[index] += 1;
            Arrays.setAll(total.get(index), i -> total.get(index)[i] + dataPointsArray.get(c).getCoordinates()[i]);
        }

        for (int j = 0; j < numberOfClusters; j++) {
            if (countOfDataPointsAssignedToEachCentroid[j] > 0) {
                for (int x = 0; x < dimensionality; x++) {
                    double newValue = total.get(j)[x] / countOfDataPointsAssignedToEachCentroid[j];
                    newValue = Math.round(newValue * 10000.0) / 10000.0;
                    total.get(j)[x] = newValue;
                }
                newCentroidList.get(j).setCoordinates(total.get(j));
            } else {
                newCentroidList.get(j).setCoordinates(total.get(j));
            }
        }
        return newCentroidList;
    }
}
