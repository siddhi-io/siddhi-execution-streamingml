package org.wso2.extension.siddhi.execution.ml.kmeansFromScratch;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Object which holds the data about coordinate values in a multidimensional space
 * dimensionality specified by the variable dimensionality
 */
public class Coordinates implements Serializable {



    private double[] coordinates;
    private int dimensionality;

    /**
     * construct with the required dimensionality of the dataPoint
     * @param dimensionality the number of dimensions required to represent
     *                       a single dataPoint
     */
    public Coordinates(int dimensionality) {
        this.dimensionality = dimensionality;
        coordinates = new double[dimensionality];
    }

    public int getDimensionality() {
        return dimensionality;
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(double[] coordinates) {
        this.coordinates = coordinates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Coordinates)) return false;

        Coordinates that = (Coordinates) o;

        return Arrays.equals(getCoordinates(), that.getCoordinates());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getCoordinates());
    }
}