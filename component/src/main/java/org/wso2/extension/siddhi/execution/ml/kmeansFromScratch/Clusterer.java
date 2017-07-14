package org.wso2.extension.siddhi.execution.ml.kmeansFromScratch;

import breeze.util.ArrayUtil;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by niruhan on 7/11/17.
 */
public class Clusterer {
    private int k;
    private int maximumIterations;
    private ArrayList<Coordinates> centroidList = new ArrayList<>();
    public ArrayList<Coordinates> newCentroidList = new ArrayList<>();
    private int dimensionality;
    private ArrayList<DataPoint> dataPointsArray;

    /**
     * Initialize no. of cluster centers, no. of iterations
     */
    public Clusterer(int k, int maximumIterations) {
        this.k = k;
        this.maximumIterations = maximumIterations;

    }

    /**
     * initializing cluster centers
     * @param dataPointsArray arraylist containing the input batch datapoints
     */
    public void initialize(ArrayList<DataPoint> dataPointsArray) {
        dimensionality = dataPointsArray.get(0).getDimensionality();
        this.dataPointsArray = dataPointsArray;
        int distinctCount = 0;
        centroidList.clear();
        newCentroidList.clear();
        for (int i = 0; i<k; i++) {
            Coordinates c = new Coordinates(dimensionality);
            newCentroidList.add(c);
        }

        for (DataPoint currentDataPoint : dataPointsArray) {
            if (distinctCount >= k) {
                break;
            }
            Coordinates coordinatesOfCurrentDataPoint = new Coordinates(currentDataPoint.getDimensionality());
            coordinatesOfCurrentDataPoint.setCoordinates(currentDataPoint.getCoordinates());
            if (!centroidList.contains(coordinatesOfCurrentDataPoint)) {
                centroidList.add(coordinatesOfCurrentDataPoint);
                distinctCount++;
            }
        }
        if (distinctCount < k) {
            k = distinctCount;
        }
    }

    /**
     * Perform clustering
     */
    public void cluster(ArrayList<DataPoint> batchDataPointsIn) {
        dataPointsArray = batchDataPointsIn;
        initialize(dataPointsArray);
        int iter = 0;
        if (dataPointsArray.size() != 0) {
            boolean centroidShifted = false;
            while(iter < maximumIterations) {
                assignToCluster(dataPointsArray);
                calculateNewCentroids();

                //test
                System.out.println("centroidList");
                for (Coordinates cen: centroidList) {
                    System.out.print(Arrays.toString(cen.getCoordinates()));
                }
                System.out.println();
                System.out.println("newCentroidList");
                for (Coordinates cen: newCentroidList) {
                    System.out.print(Arrays.toString(cen.getCoordinates()));
                }
                //test end

                centroidShifted = !centroidList.equals(newCentroidList);
                System.out.println();
                System.out.println(centroidShifted);
                if (centroidShifted==false) {
                    break;
                }
                for (int i = 0; i<k; i++) {
                    centroidList.get(i).setCoordinates(newCentroidList.get(i).getCoordinates());
                }
                iter++;
            }
        }
    }

    /**
     * finds the nearest centroid to each data point in the input array
     * @param dataPointsArray arraylist containing datapoints for which we need to assign centroids
     */
    public void assignToCluster(ArrayList<DataPoint> dataPointsArray) {
        for (DataPoint currentDataPoint : dataPointsArray) {
            Coordinates associatedCentroid = findAssociatedCentroid(currentDataPoint);
            currentDataPoint.setAssociatedCentroid(associatedCentroid);
        }
    }

    /**
     * finds the nearest centroid to a given DataPoint
     * @param currentDatapoint input DataPoint to which we need to find nearest centroid
     * @return centroid - the nearest centroid to the input DataPoint
     */
    public Coordinates findAssociatedCentroid(DataPoint currentDatapoint) {
        double minDistance = euclideanDistance(centroidList.get(0), currentDatapoint);
        Coordinates associatedCentroid = centroidList.get(0);
        for (Coordinates centroid: centroidList) {
            double dist = euclideanDistance(centroid, currentDatapoint);
            if (dist < minDistance) {
                minDistance = dist;
                associatedCentroid = centroid;
            }
        }
        return associatedCentroid;
    }

    /*public Object[] getAssociatedCentroidInfo(DataPoint currentDatapoint) {
        double minDistance = euclideanDistance(centroidList.get(0), currentDatapoint);
        Coordinates associatedCentroid = centroidList.get(0);
        for (Coordinates centroid: centroidList) {
            double dist = euclideanDistance(centroid, currentDatapoint);
            if (dist < minDistance) {
                minDistance = dist;
                associatedCentroid = centroid;
            }
        }
        double[] md = {minDistance};
        double[] associatedCentroidInfo = ArrayUtils.addAll(md, associatedCentroid.getCoordinates());

        //return associatedCentroid;
    }*/ //TODO : find a way to pass double array as object array

    /**
     * finds the euclidean distance between two input points of equal dimension
     * @param point1 input point one
     * @param point2 input point two
     * @return euclidean distance between point1 and point2
     */
    public double euclideanDistance(Coordinates point1, Coordinates point2) {
        double sum = 0.0;
        for (int i=0; i<dimensionality; i++) {
            sum += Math.pow((point1.getCoordinates()[i] - point2.getCoordinates()[i]),2);
        }
        double dist = Math.sqrt(sum);
        dist = Math.round(dist * 10000.0) / 10000.0;
        return dist;
    }

    /**
     * returns the list of centroids
     * @return centroids list
     */
    public ArrayList<Coordinates> getCentroidList() {
        return centroidList;
    }

    public ArrayList<Coordinates> calculateNewCentroids() {

        ArrayList<double[]> total = new ArrayList<>();
        int[] count = new int[k];
        for (int i=0; i<k; i++) {
            count[i] = 0;
            total.add(new double[dimensionality]);
        }

        /*DataPoint d;
        d = dataPointsArray.get(0);
        System.out.print(Arrays.toString(d.getCoordinates()));*/

        for (int y = 0; y<dataPointsArray.size(); y++) {
            //System.out.println("Hi3");
            Coordinates associatedCen = dataPointsArray.get(y).getAssociatedCentroid();
            int index = centroidList.indexOf(associatedCen);
            int c = y;
            count[index] += 1;
            Arrays.setAll(total.get(index), i -> total.get(index)[i] + dataPointsArray.get(c).getCoordinates()[i]);
        }

        for (int j=0; j<k; j++) {
            for (int x=0; x<dimensionality; x++) {
                double newValue = total.get(j)[x] / count[j];
                newValue = Math.round(newValue * 10000.0) / 10000.0;
                total.get(j)[x] = newValue;

                //System.out.println(Arrays.toString(total.get(j)));
            }
            newCentroidList.get(j).setCoordinates(total.get(j));
        }
        return newCentroidList;
    }




}
