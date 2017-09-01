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


import org.wso2.extension.siddhi.execution.streamingml.util.Coordinates;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * stores info about the kmeans model
 */
public class KMeansModel implements Serializable {
    static final long serialVersionUID = 1L;
    private List<Coordinates> centroidList;

    public KMeansModel() {
        centroidList = new LinkedList<>();
    }

    public KMeansModel(List<Coordinates> centroidList) {
        this.centroidList = centroidList;
    }

    public synchronized List<Coordinates> getCentroidList() {
        return centroidList;
    }

    public synchronized void setCentroidList(List<Coordinates> centroidList) {
        this.centroidList = centroidList;
    }

    public synchronized void clear() {
        centroidList.clear();
    }

    public synchronized boolean contains(Coordinates x) {
        return centroidList.contains(x);
    }

    public synchronized void add(Coordinates x) {
        centroidList.add(x);
    }

    public synchronized void update(int index, double[] x) {
        centroidList.get(index).setCoordinates(x);
    }

    public synchronized int size() {
        return centroidList.size();
    }

    public synchronized double[] getCoordinatesOfCentroid(int index) {
        return centroidList.get(index).getCoordinates();
    }

    public synchronized Coordinates getCentroid(int index) {
        return centroidList.get(index);
    }

    public synchronized int indexOf(Coordinates x) {
        return centroidList.indexOf(x);
    }
}
