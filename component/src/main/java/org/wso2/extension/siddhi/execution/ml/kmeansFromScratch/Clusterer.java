package org.wso2.extension.siddhi.execution.ml.kmeansFromScratch;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by niruhan on 7/11/17.
 */
public class Clusterer {
    private int k;
    private int maximumIterations;
    private ArrayList<Coordinates> centroidList;
    private ArrayList<Coordinates> newCentroidList;
    private int dimensionality;
    private ArrayList<DataPoint> dataPointsArray;
    private int[] count;

    /**
     * Initialize no. of cluster centers, no. of iterations
     */
    public Clusterer(int k, int maximumIterations) {
        this.k = k;
        this.maximumIterations = maximumIterations;
        count = new int[k];
        centroidList = new ArrayList<>();
        newCentroidList = new ArrayList<>();

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
    }

    /**
     * Perform clustering
     */
    public void cluster(ArrayList<DataPoint> batchDataPointsIn) {
        System.out.println("initial Clustering");
        dataPointsArray = batchDataPointsIn;
        initialize(dataPointsArray);
        int iter = 0;
        if (dataPointsArray.size() != 0 & (centroidList.size() == k)) {
            boolean centroidShifted = false;
            while(iter < maximumIterations) {
                assignToCluster(dataPointsArray);
                calculateNewCentroids();

                centroidShifted = !centroidList.equals(newCentroidList);
                if (!centroidShifted) {
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
     * After the first clustering this method can be used to incrementally update centroidList
     * in real time. This method takes in the new set of datapoints and decayRate as inputs
     * and calculates the centroids of the new set. Then new centroids are calculated using
     * newAvg = oldAvg + decayRate * batchAvg
     * @param batchDataPointsIn
     * @param decayRate should be in [0,1]
     */
    public void updateCluster(ArrayList<DataPoint> batchDataPointsIn, float decayRate) {
        System.out.println("Updating cluster");

        dataPointsArray = batchDataPointsIn;

        int iter = 0;
        if (dataPointsArray.size() != 0) {

            //when number of elements in centroid list is less than k
            if (centroidList.size() < k) {
                int distinctCount = centroidList.size();
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
            }

            if (centroidList.size() == k) {
                ArrayList<Coordinates> oldCentroidList = new ArrayList<>(k);
                for (int i = 0; i<k; i++) {
                    Coordinates c = new Coordinates(dimensionality);
                    c.setCoordinates(centroidList.get(i).getCoordinates());
                    oldCentroidList.add(c);
                }
                boolean centroidShifted = false;
                while (iter < maximumIterations) {
                    assignToCluster(dataPointsArray);
                    System.out.println("data points array");
                    for (Coordinates c: dataPointsArray) {
                        System.out.print(Arrays.toString(c.getCoordinates()));
                    }
                    System.out.println();
                    calculateNewCentroids();

                    centroidShifted = !centroidList.equals(newCentroidList);
                    //System.out.println(Arrays.toString(centroidList.get(0).getCoordinates()));
                    //System.out.println(Arrays.toString(newCentroidList.get(0).getCoordinates()) + Arrays.toString(newCentroidList.get(1).getCoordinates()));

                    System.out.println("centroid list");
                    for (Coordinates c: centroidList) {
                        System.out.print(Arrays.toString(c.getCoordinates()));
                    }
                    System.out.println();
                    System.out.println("new centroid list");
                    for (Coordinates c: newCentroidList) {
                        System.out.print( Arrays.toString(c.getCoordinates()));
                    }
                    System.out.println();

                    System.out.println("Centroid shifted? = " + centroidShifted);
                    if (!centroidShifted) {
                        break;
                    }
                    for (int i = 0; i < k; i++) {
                        centroidList.get(i).setCoordinates(newCentroidList.get(i).getCoordinates());
                    }
                    iter++;
                }
                System.out.println("old centroid list");
                for (Coordinates c: oldCentroidList) {
                    System.out.print(Arrays.toString(c.getCoordinates()));
                }
                System.out.println();
                for (int i = 0; i<k; i++) {
                    if (count[i] > 0) {
                        double[] weightedCoordinates = new double[dimensionality];
                        double[] oldCoordinates = oldCentroidList.get(i).getCoordinates();
                        double[] newCoordinates = centroidList.get(i).getCoordinates();
                        Arrays.setAll(weightedCoordinates, j -> Math.round(((1 - decayRate) * oldCoordinates[j] + decayRate * newCoordinates[j]) * 10000.0) / 10000.0);
                        System.out.println("weighted" + Arrays.toString(weightedCoordinates));
                        centroidList.get(i).setCoordinates(weightedCoordinates);
                    } else {
                        centroidList.get(i).setCoordinates(oldCentroidList.get(i).getCoordinates());
                    }
                }
                System.out.println("weighted centroid list");
                for (Coordinates c: centroidList) {
                    System.out.print(Arrays.toString(c.getCoordinates()));
                }
                System.out.println();
            }
        }

    }

    /**
     * finds the nearest centroid to each data point in the input array
     * @param dataPointsArray arraylist containing datapoints for which we need to assign centroids
     */
    private void assignToCluster(ArrayList<DataPoint> dataPointsArray) {
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
    private Coordinates findAssociatedCentroid(DataPoint currentDatapoint) {
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

    /**
     * similar to findAssociatedCentroid method but return an Object[] array with the distance
     * to closest centroid and the coordinates of the closest centroid
     * @param currentDatapoint the input dataPoint for which the closest centroid needs to be found
     * @return an Object[] array as mentioned above
     */
    public Object[] getAssociatedCentroidInfo(DataPoint currentDatapoint) {
        double minDistance = euclideanDistance(centroidList.get(0), currentDatapoint);
        Coordinates associatedCentroid = centroidList.get(0);
        for (Coordinates centroid: centroidList) {
            double dist = euclideanDistance(centroid, currentDatapoint);
            if (dist < minDistance) {
                minDistance = dist;
                associatedCentroid = centroid;
            }
        }

        List<Double> associatedCentroidInfoList = new ArrayList<Double>();
        associatedCentroidInfoList.add(minDistance);
        for (double x: associatedCentroid.getCoordinates()) {
            associatedCentroidInfoList.add(x);
        }

        Object[] associatedCentroidInfo = new Object[associatedCentroidInfoList.size()];

        associatedCentroidInfoList.toArray(associatedCentroidInfo);

        return associatedCentroidInfo;
    }

    /**
     * finds the euclidean distance between two input points of equal dimension
     * @param point1 input point one
     * @param point2 input point two
     * @return euclidean distance between point1 and point2
     */
    private double euclideanDistance(Coordinates point1, Coordinates point2) {
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

    /**
     * after assigning data points to closest centroids this method calculates new centroids using
     * the assigned points
     * @return returns an array list of coordinate objects each representing a centroid
     */
    private void calculateNewCentroids() {

        ArrayList<double[]> total = new ArrayList<>();
        for (int i=0; i<k; i++) {
            count[i] = 0;
            total.add(new double[dimensionality]);
        }


        for (int y = 0; y<dataPointsArray.size(); y++) {
            Coordinates associatedCen = dataPointsArray.get(y).getAssociatedCentroid();
            int index = centroidList.indexOf(associatedCen);
            int c = y;
            count[index] += 1;
            Arrays.setAll(total.get(index), i -> total.get(index)[i] + dataPointsArray.get(c).getCoordinates()[i]);
        }

        for (int j=0; j<k; j++) {
            if (count[j] > 0) {
                for (int x = 0; x < dimensionality; x++) {
                    double newValue = total.get(j)[x] / count[j];
                    newValue = Math.round(newValue * 10000.0) / 10000.0;
                    total.get(j)[x] = newValue;
                }
                newCentroidList.get(j).setCoordinates(total.get(j));
            } else {
                newCentroidList.get(j).setCoordinates(total.get(j));
            }
        }
    }




}
