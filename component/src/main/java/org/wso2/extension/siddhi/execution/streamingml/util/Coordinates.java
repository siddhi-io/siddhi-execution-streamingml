/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.streamingml.util;

import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Object which holds the data about coordinate values in a multidimensional space
 * dimensionality specified by the variable dimensionality
 */
public class Coordinates implements Serializable {
    static final long serialVersionUID = 1L;
    private double[] coordinates;

    public double[] getCoordinates() {
        if (coordinates != null) {
            return coordinates.clone();
        } else {
            throw new SiddhiAppValidationException("No coordinates have been set. Hence null return value.");
        }
    }

    public void setCoordinates(double[] coordinates) {
        if (this.coordinates != null) {
            if (this.coordinates.length == coordinates.length) {
                this.coordinates = coordinates.clone();
            } else {
                throw new SiddhiAppValidationException("The dimensionality of the coordinate is " +
                        this.coordinates.length + " but the dimensionality of the received array is " +
                        coordinates.length);
            }
        } else  {
            this.coordinates = coordinates.clone();
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Coordinates)) {
            return false;
        }

        Coordinates that = (Coordinates) o;
        return Arrays.equals(getCoordinates(), that.getCoordinates());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getCoordinates());
    }
}
