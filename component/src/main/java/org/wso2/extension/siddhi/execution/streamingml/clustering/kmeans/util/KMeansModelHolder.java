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

import java.util.HashMap;
import java.util.Map;

/**
 * singleton to store the kmeans models
 */
public class KMeansModelHolder {
    private static final KMeansModelHolder instance = new KMeansModelHolder();
    private Map<String, KMeansModel> kMeansModelMap = new HashMap<>();

    private KMeansModelHolder(){
    }

    public static KMeansModelHolder getInstance() {
        return instance;
    }

    public Map<String, KMeansModel> getKMeansModelMap() {
        return kMeansModelMap;
    }

    public void setKMeansModelMap(Map<String, KMeansModel> kMeansModelMap) {
        this.kMeansModelMap = kMeansModelMap;
    }

    public KMeansModel getKMeansModel(String name) {
        return kMeansModelMap.get(name);
    }

    public void addKMeansModel(String name, KMeansModel model) {
        kMeansModelMap.put(name, model);
    }

    public Map<String, KMeansModel> getClonedKMeansModelMap() {
        Map<String, KMeansModel> clonedMap = new HashMap<>();
        for (Map.Entry<String, KMeansModel> entry: kMeansModelMap.entrySet()) {
            clonedMap.put(entry.getKey(), new KMeansModel(entry.getValue().getCentroidList()));
        }

        return clonedMap;
    }
}
