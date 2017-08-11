package org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util;

import org.wso2.extension.siddhi.execution.ml.util.Coordinates;
import org.wso2.extension.siddhi.execution.ml.util.MathUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Clusterer {
    private int k;
    private int maximumIterations;
    private KMeansModel model;
    private ArrayList<Coordinates> newCentroidList;
    private int dimensionality;
    private ArrayList<DataPoint> dataPointsArray;
    private int[] count;
    private final static Logger logger = Logger.getLogger(Clusterer.class.getName());

    /**
     * Initialize no. of cluster centers, no. of iterations
     */
    public Clusterer(int k, int maximumIterations, String modelName) {
        model = KMeansModelHolder.getInstance().getKMeansModel(modelName);
        if (model == null) {
            model = new KMeansModel();
            KMeansModelHolder.getInstance().addKMeansModel(modelName, model);
        }
        this.k = k;
        this.maximumIterations = maximumIterations;
        count = new int[k];
        newCentroidList = new ArrayList<>();
        logger.setLevel(Level.ALL);

    }

    public void setModel(KMeansModel m) {
        model = m;
    }

    /**
     * initializing cluster centers
     * @param dataPointsArray arraylist containing the input batch datapoints
     */
    public void initialize(ArrayList<DataPoint> dataPointsArray) {
        dimensionality = dataPointsArray.get(0).getDimensionality();
        this.dataPointsArray = dataPointsArray;
        int distinctCount = 0;
        model.clear();
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
            if (!model.contains(coordinatesOfCurrentDataPoint)) {
                model.add(coordinatesOfCurrentDataPoint);
                distinctCount++;
            }
        }
    }

    /**
     * Perform clustering
     */
    public void cluster(ArrayList<DataPoint> batchDataPointsIn) {
        logger.info("initial Clustering");
        dataPointsArray = batchDataPointsIn;
        initialize(dataPointsArray);
        int iter = 0;
        if (dataPointsArray.size() != 0 & (model.size() == k)) {
            boolean centroidShifted = false;
            while(iter < maximumIterations) {
                assignToCluster(dataPointsArray);
                calculateNewCentroids();

                centroidShifted = !model.getCentroidList().equals(newCentroidList);
                if (!centroidShifted) {
                    break;
                }
                for (int i = 0; i<k; i++) {
                    model.update(i, newCentroidList.get(i).getCoordinates());
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
        logger.info("Updating cluster");
        StringBuilder s;

        ArrayList<Coordinates> intermediateCentroidList = new ArrayList<>();

        dataPointsArray = new ArrayList(batchDataPointsIn);

        int iter = 0;
        if (dataPointsArray.size() != 0) {

            //when number of elements in centroid list is less than k
            if (model.size() < k) {
                int distinctCount = model.size();
                for (DataPoint currentDataPoint : dataPointsArray) {
                    if (distinctCount >= k) {
                        break;
                    }
                    Coordinates coordinatesOfCurrentDataPoint = new Coordinates(currentDataPoint.getDimensionality());
                    coordinatesOfCurrentDataPoint.setCoordinates(currentDataPoint.getCoordinates());
                    if (!model.contains(coordinatesOfCurrentDataPoint)) {
                        model.add(coordinatesOfCurrentDataPoint);
                        distinctCount++;
                    }
                }
            }

            if (model.size() == k) {
                ArrayList<Coordinates> oldCentroidList = new ArrayList<>(k);
                for (int i = 0; i<k; i++) {
                    Coordinates c = new Coordinates(dimensionality);
                    Coordinates c1 = new Coordinates(dimensionality);
                    c.setCoordinates(model.getCoordinatesOfCentroid(i));
                    c1.setCoordinates(model.getCoordinatesOfCentroid(i));
                    oldCentroidList.add(c);
                    intermediateCentroidList.add(c1);
                }
                boolean centroidShifted = false;
                while (iter < maximumIterations) {
                    assignToCluster(dataPointsArray);
                    logger.info("data points array");
                    s= new StringBuilder();
                    for (Coordinates c: dataPointsArray) {
                         s.append(Arrays.toString(c.getCoordinates()));

                    }
                    logger.info(s.toString());
                    calculateNewCentroids();

                    centroidShifted = !intermediateCentroidList.equals(newCentroidList);

                    logger.info("centroid list");
                    s= new StringBuilder();
                    for (Coordinates c: model.getCentroidList()) {
                        s.append(Arrays.toString(c.getCoordinates()));
                    }
                    logger.info(s.toString());
                    logger.info("new centroid list");
                    s= new StringBuilder();
                    for (Coordinates c: newCentroidList) {
                        s.append(Arrays.toString(c.getCoordinates()));
                    }
                    logger.info(s.toString());

                    logger.info("Centroid shifted? = " + centroidShifted);
                    if (!centroidShifted) {
                        break;
                    }
                    for (int i = 0; i < k; i++) {
                        intermediateCentroidList.get(i).setCoordinates(newCentroidList.get(i).getCoordinates());
                    }
                    iter++;
                }
                logger.info("old centroid list");
                s= new StringBuilder();
                for (Coordinates c: oldCentroidList) {
                    s.append(Arrays.toString(c.getCoordinates()));
                }
                logger.info(s.toString());
                for (int i = 0; i<k; i++) {
                    if (count[i] > 0) {
                        double[] weightedCoordinates = new double[dimensionality];
                        double[] oldCoordinates = oldCentroidList.get(i).getCoordinates();
                        double[] newCoordinates = intermediateCentroidList.get(i).getCoordinates();
                        Arrays.setAll(weightedCoordinates, j -> Math.round(((1 - decayRate) * oldCoordinates[j] + decayRate * newCoordinates[j]) * 10000.0) / 10000.0);
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
                s=new StringBuilder();
                for (Coordinates c: model.getCentroidList()) {
                    s.append(Arrays.toString(c.getCoordinates()));
                }
                logger.info(s.toString());
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
        double minDistance = MathUtil.euclideanDistance(model.getCoordinatesOfCentroid(0), currentDatapoint.getCoordinates());
        Coordinates associatedCentroid = model.getCentroid(0);
        for (int i=0; i<model.size(); i++) {
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
     * @param currentDatapoint the input dataPoint for which the closest centroid needs to be found
     * @return an Object[] array as mentioned above
     */
    public Object[] getAssociatedCentroidInfo(DataPoint currentDatapoint) {

        Coordinates associatedCentroid = findAssociatedCentroid(currentDatapoint);
        double minDistance = MathUtil.euclideanDistance(currentDatapoint.getCoordinates(), associatedCentroid.getCoordinates());
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
     * returns the list of centroids
     * @return centroids list
     */
    /*public ArrayList<Coordinates> getCentroidList() {
        return centroidList;
    }*/

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
            int index = model.indexOf(associatedCen);
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
