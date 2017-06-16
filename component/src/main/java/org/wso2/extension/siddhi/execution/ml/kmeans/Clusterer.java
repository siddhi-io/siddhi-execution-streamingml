package org.wso2.extension.siddhi.execution.ml.kmeans;

import java.util.ArrayList;


public class Clusterer {
    private  int numberOfClusters;
    private  int maximumIterations;
    private ArrayList<Double> center = new ArrayList<>();
    private ArrayList<Double> centerOld = new ArrayList<>();


    private ArrayList<ClusterObject> clusterData;


    /**
     * Initialize no. of cluster centers, no. of iterations, clusterGroup array
     */
    public Clusterer(int clusters, int iterations) {
        numberOfClusters = clusters;
        maximumIterations = iterations;
    }

    /**
     * Perform clustering
     */
    public void cluster(ArrayList<ClusterObject> data) {
        clusterData = data;
        initialize(clusterData);
        int iter = 0;
        if (data.size() != 0) {
            while (!center.equals(centerOld) && iter < maximumIterations){
                assignToCluster(data);
                reinitializeCluster();
                iter++;
            }


        }
    }

    /**
     * initializing cluster centers
     */
    public void initialize(ArrayList<ClusterObject> data) {
       int distinctCount = 0;

        center.clear();
        for (int i = 0; i < data.size(); i++) {
            if (distinctCount >= numberOfClusters) {
                break;
            }
            double value = data.get(i).getValue();
            if (!center.contains(value)) {

                center.add(value);
                distinctCount++;
            }
        }
        if (distinctCount < numberOfClusters) {
            numberOfClusters = distinctCount;
        }

    }


    /**
     * reinitialize the cluster centres and store the old ones
     */
    private void reinitializeCluster() {
        double[] average = average(clusterData);
        centerOld = new ArrayList<>();
        for (int i = 0; i < numberOfClusters; i++) {
            centerOld.add(i, center.get(i));
            center.set(i, average[i]);
        }
    }


    /**
     * base on the data points assigned to the cluster, recalculates the cluster center
     * @param clusterData arraylist containing clusterObjects to be clustered
     * @return the new cluster center
     */
    private double[] average(ArrayList<ClusterObject> clusterData) {
        double[] total = new double[numberOfClusters];
        int[] count = new int[numberOfClusters];
        for (int i = 0; i < numberOfClusters; i++) {
            total[i] = 0;
            count[i] = 0;
        }
        for (int i = 0; i < clusterData.size(); i++) {
            int centerIndex = clusterData.get(i).getIndex();
            count[centerIndex] += 1;
            total[centerIndex] += clusterData.get(i).getValue();
        }
        for (int i = 0; i < numberOfClusters; i++) {
            total[i] = total[i] / count[i];
        }
        return (total);
    }

    /**
     * calculates the nearest center to each data point and adds the data to the cluster of respective center
     * @param data - arraylist containing clusterObjects to be assigned to new clusters
     */
    private void assignToCluster(ArrayList<ClusterObject> data) {
        Object[] output;
        double value;
        for (int i = 0; i < data.size(); i++) {
            value = data.get(i).getValue();
            output = getCenter(value);
            int index = (Integer) output[1];
            data.get(i).setIndex(index);
        }
    }


    /**
     * @param value - the value for which the center it belongs to should be returned
     * @return center - object array containing the matched cluster center, the index of the cluster center,
     *                  and the distance to the cluster center
     */

    public Object[] getCenter(double value) {
        double centerValue, difference, currentDifference;
        int index;
        difference = Math.abs(center.get(0) - value);
        difference = Math.round(difference * 10000.0) / 10000.0;
        centerValue = center.get(0);
        index = 0;
        for (int j = 1; j < numberOfClusters; j++) {
            currentDifference = Math.abs(center.get(j) - value);
            currentDifference = Math.round(currentDifference * 10000.0) / 10000.0;
            if (difference > currentDifference) {
                difference = currentDifference;
                centerValue = center.get(j);
                index = j;
            } else if (currentDifference == difference) {
                if (centerValue < center.get(j)) {
                    centerValue = center.get(j);
                    index = j;
                }
            }
        }
        Object[] center = {centerValue, index, difference};
        return center;
    }


}
