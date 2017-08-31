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

package org.wso2.extension.siddhi.execution.ml.clustering.kmeans.util;

import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

public class Trainer implements Runnable {

    private final static Logger logger = Logger.getLogger(Trainer.class.getName());

    private Clusterer clusterer;
    private List<DataPoint> dataPointsArray;
    private float decayRate;

    public Trainer(Clusterer clusterer, List<DataPoint> dataPointsArray, float decayRate) {
        this.clusterer=clusterer; //TODO: proper naming of params - done
        this.dataPointsArray=dataPointsArray;
        this.decayRate=decayRate;
    }

    @Override
    public void run() {
        if (logger.isDebugEnabled()) {
            logger.debug("running trainer thread"); //TODO: debug logger - done
        }
        clusterer.updateCluster(dataPointsArray, decayRate);
    }
}
