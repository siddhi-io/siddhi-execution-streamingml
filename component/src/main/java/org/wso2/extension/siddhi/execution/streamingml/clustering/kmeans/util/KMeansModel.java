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

import org.apache.log4j.Logger;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * stores info about the kmeans model
 */
public class KMeansModel implements Serializable {
    static final long serialVersionUID = 1L;
    private List<Cluster> clusterList;
    private static final Logger logger = Logger.getLogger(KMeansModel.class.getName());

    public KMeansModel() {
        clusterList = new LinkedList<>();
        //logger.setLevel(Level.ALL);
    }

    public KMeansModel(List<Cluster> clusterList) {
        this.clusterList = clusterList;
    }

    public synchronized List<Cluster> getClusterList() {
        return clusterList;
    }

    public synchronized void setClusterList(List<Cluster> clusterList) {
        this.clusterList = clusterList;
    }

    public synchronized void clear() {
        clusterList.clear();
    }

    public synchronized void clearClusterMembers() {
        for (Cluster c: clusterList) {
            if (c != null) {
                c.clearDataPointsInCluster();
            }
        }
    }

    public synchronized boolean contains(DataPoint x) {
        for (Cluster c: clusterList) {
            if (c.getCentroid().equals(x)) {
                return true;
            }
        }
        return false;
    }

    public synchronized void add(DataPoint x) {
        if (logger.isDebugEnabled()) {
            logger.debug("adding a new cluster with centroid " + Arrays.toString(x.getCoordinates()));
        }
        Cluster c = new Cluster(x);
        clusterList.add(c);
    }

    public synchronized void update(int index, double[] x) {
        clusterList.get(index).getCentroid().setCoordinates(x);
    }

    public synchronized int size() {
        return clusterList.size();
    }

    public synchronized double[] getCoordinatesOfCentroidOfCluster(int index) {
        return clusterList.get(index).getCentroid().getCoordinates();
    }

    public synchronized DataPoint getCentroidOfCluster(int index) {
        return clusterList.get(index).getCentroid();
    }

    public synchronized int indexOf(DataPoint x) {
        for (int i = 0; i < clusterList.size(); i++) {
            if (clusterList.get(i).getCentroid().equals(x)) {
                return i;
            }
        }
        return -1;
    }

    public synchronized String getModelInfo() {
        StringBuilder s = new StringBuilder();
        for (Cluster c: clusterList) {
            s.append(Arrays.toString(c.getCentroid().getCoordinates())).append(" with members : ")
                    .append(c.getMemberInfo()).append("\n");
        }
        return s.toString();
    }
}
