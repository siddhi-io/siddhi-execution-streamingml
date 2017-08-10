package org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util;


import org.wso2.extension.siddhi.execution.ml.util.Coordinates;

import java.io.Serializable;
import java.util.ArrayList;

public class KMeansModel implements Serializable {
    private ArrayList<Coordinates> centroidList;

    public KMeansModel() {
        centroidList = new ArrayList<>();
    }

    public KMeansModel(ArrayList<Coordinates> centroidList) {
        this.centroidList = centroidList;
    }

    public synchronized ArrayList<Coordinates> getCentroidList() {
        return centroidList;
    }

    public synchronized void setCentroidList(ArrayList<Coordinates> centroidList) {
        this.centroidList = centroidList;
    }

    public synchronized void clear() {
        centroidList.clear();
    }

    public synchronized boolean contains(Coordinates x) {
        return centroidList.contains(x);
    }

    public synchronized void add(Coordinates x) {
        centroidList.add(x);
    }

    public synchronized void update(int index, double[] x) {
        centroidList.get(index).setCoordinates(x);
    }

    public synchronized int size() {
        return centroidList.size();
    }

    public synchronized double[] getCoordinatesOfCentroid(int index) {
        return centroidList.get(index).getCoordinates();
    }

    public synchronized Coordinates getCentroid(int index) {
        return centroidList.get(index);
    }

    public synchronized int indexOf(Coordinates x) {
        return centroidList.indexOf(x);
    }
}
