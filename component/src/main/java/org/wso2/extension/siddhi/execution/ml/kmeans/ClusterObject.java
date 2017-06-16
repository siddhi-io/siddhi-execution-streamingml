package org.wso2.extension.siddhi.execution.ml.kmeans;

import java.io.Serializable;

public class ClusterObject implements Serializable {

    /**
     * Object which holds the data to be clustered and the
     * index of the centroid to which it belongs to
     */

    private double value;
    private int index;


    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterObject that = (ClusterObject) o;

        return Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(value);
        return (int) (temp ^ (temp >>> 32));
    }
}
