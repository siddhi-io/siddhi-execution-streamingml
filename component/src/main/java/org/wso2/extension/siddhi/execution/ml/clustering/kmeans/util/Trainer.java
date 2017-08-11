package org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Trainer implements Runnable {

    private final static Logger logger = Logger.getLogger(Trainer.class.getName());

    private Clusterer clusterer;
    private ArrayList<DataPoint> dataPointsArray;
    float decayRate;

    public Trainer(Clusterer c, ArrayList<DataPoint> a, float d) {
        clusterer=c;
        dataPointsArray=a;
        decayRate=d;
        logger.setLevel(Level.ALL);
    }

    @Override
    public void run() {
        logger.info("running trainer thread");
        clusterer.updateCluster(dataPointsArray, decayRate);
    }
}
