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

/**
 * Object which holds the data to be clustered and the
 * associatedCentroid, the centroid to which it belongs to
 */
public class DataPoint extends Coordinates {
    static final long serialVersionUID = 1L;
    private Coordinates associatedCentroid;

    public Coordinates getAssociatedCentroid() {
        return associatedCentroid;
    }

    public void setAssociatedCentroid(Coordinates associatedCentroid) {
        this.associatedCentroid = associatedCentroid;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
