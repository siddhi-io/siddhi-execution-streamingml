package org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util;

import java.util.ArrayList;

public class Trainer implements Runnable {

    private Clusterer clusterer;
    private ArrayList<DataPoint> dataPointsArray;
    float decayRate;

    public Trainer(Clusterer c, ArrayList<DataPoint> a, float d) {
        clusterer=c;
        dataPointsArray=a;
        decayRate=d;
    }

    @Override
    public void run() {
        System.out.println("running trainer thread");
        clusterer.updateCluster(dataPointsArray, decayRate);
    }
}
