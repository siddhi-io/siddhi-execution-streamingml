package org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util;

import org.wso2.extension.siddhi.execution.ml.util.Coordinates;
import java.io.Serializable;


/**
 * Object which holds the data to be clustered and the
 * associatedCentroid, the centroid to which it belongs to
 */
public class DataPoint extends Coordinates implements Serializable {


    private Coordinates associatedCentroid;

    /**
     * construct with the required dimensionality of the dataPoint
     * @param dimensionality the number of dimensions required to represent
     *                       a single dataPoint
     */
    public DataPoint(int dimensionality) {
        super(dimensionality);
    }


    public Coordinates getAssociatedCentroid() {
        return associatedCentroid;
    }

    public void setAssociatedCentroid(Coordinates associatedCentroid) {
        this.associatedCentroid = associatedCentroid;
    }
}
