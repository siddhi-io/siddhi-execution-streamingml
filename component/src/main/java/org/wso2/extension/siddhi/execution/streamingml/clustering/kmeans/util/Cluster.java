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

package org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * This class represents a single cluster in the model
 */
public class Cluster implements Serializable {
    private static final long serialVersionUID = 1917517756301230642L;
    private DataPoint centroid;
    private List<DataPoint> dataPointsInCluster;

    Cluster(DataPoint centroid) {
        this.centroid = centroid;
        dataPointsInCluster = new LinkedList<>();
    }

    DataPoint getCentroid() {
        return centroid;
    }

    List<DataPoint> getDataPointsInCluster() {
        return dataPointsInCluster;
    }

    void clearDataPointsInCluster() {
        dataPointsInCluster.clear();
    }

    void addToCluster(DataPoint currentDataPoint) {
        dataPointsInCluster.add(currentDataPoint);
    }

    String getMemberInfo() {
        StringBuilder s = new StringBuilder();
        for (DataPoint d: dataPointsInCluster) {
            s.append(Arrays.toString(d.getCoordinates()));
        }
        return s.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Cluster)) {
            return false;
        }

        Cluster that = (Cluster) o;
        return Arrays.equals(centroid.getCoordinates(), that.getCentroid().getCoordinates());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(centroid.getCoordinates());
    }
}
