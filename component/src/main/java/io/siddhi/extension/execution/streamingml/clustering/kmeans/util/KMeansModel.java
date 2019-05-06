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

package io.siddhi.extension.execution.streamingml.clustering.kmeans.util;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * stores info about the kmeans model
 */
public class KMeansModel implements Serializable {
    private static final long serialVersionUID = 7997333339345312740L;
    private List<Cluster> clusterList;
    private boolean trained;
    private static final Logger logger = Logger.getLogger(KMeansModel.class.getName());

    public KMeansModel() {
        clusterList = new LinkedList<>();
    }

    public boolean isTrained() {
        return trained;
    }

    void setTrained() {
        this.trained = true;
    }

    synchronized List<Cluster> getClusterList() {
        return clusterList;
    }

    synchronized void setClusterList(List<Cluster> clusterList) {
        this.clusterList = clusterList;
    }

    public synchronized void clear() {
        clusterList.clear();
    }

    synchronized void clearClusterMembers() {
        for (Cluster c: clusterList) {
            if (c != null) {
                c.clearDataPointsInCluster();
            }
        }
    }

    synchronized boolean contains(DataPoint x) {
        for (Cluster c: clusterList) {
            if (c.getCentroid().equals(x)) {
                return true;
            }
        }
        return false;
    }

    synchronized void add(DataPoint x) {
        if (logger.isDebugEnabled()) {
            logger.debug("adding a new cluster with centroid " + Arrays.toString(x.getCoordinates()));
        }
        Cluster c = new Cluster(x);
        clusterList.add(c);
    }

    synchronized int size() {
        return clusterList.size();
    }

    synchronized double[] getCoordinatesOfCentroidOfCluster(int index) {
        return clusterList.get(index).getCentroid().getCoordinates();
    }

    synchronized String getModelInfo() {
        StringBuilder s = new StringBuilder();
        for (Cluster c: clusterList) {
            s.append(Arrays.toString(c.getCentroid().getCoordinates())).append(" with members : ")
                    .append(c.getMemberInfo()).append("\n");
        }
        return s.toString();
    }
}
