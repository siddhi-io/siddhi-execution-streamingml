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

    public ArrayList<Coordinates> getCentroidList() {
        return centroidList;
    }

    public void setCentroidList(ArrayList<Coordinates> centroidList) {
        this.centroidList = centroidList;
    }

    public void clear() {
        centroidList.clear();
    }

    public boolean contains(Coordinates x) {
        return centroidList.contains(x);
    }

    public void add(Coordinates x) {
        centroidList.add(x);
    }

    public void update(int index, double[] x) {
        centroidList.get(index).setCoordinates(x);
    }

    public int size() {
        return centroidList.size();
    }

    public double[] getCoordinatesOfCentroid(int index) {
        return centroidList.get(index).getCoordinates();
    }

    public Coordinates getCentroid(int index) {
        return centroidList.get(index);
    }

    public int indexOf(Coordinates x) {
        return centroidList.indexOf(x);
    }
}
