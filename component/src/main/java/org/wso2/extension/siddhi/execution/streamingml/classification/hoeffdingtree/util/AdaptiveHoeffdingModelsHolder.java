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
package org.wso2.extension.siddhi.execution.streamingml.classification.hoeffdingtree.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Data holder which keeps the instances of @{@link AdaptiveHoeffdingTreeModel}
 */
public class AdaptiveHoeffdingModelsHolder {
    private static final AdaptiveHoeffdingModelsHolder instance = new AdaptiveHoeffdingModelsHolder();

    /**
     * Key - name of the model
     * Value - @{@link AdaptiveHoeffdingTreeModel}
     */
    private Map<String, AdaptiveHoeffdingTreeModel> hoeffdingModelMap = new HashMap();

    private AdaptiveHoeffdingModelsHolder() {
    }

    public static AdaptiveHoeffdingModelsHolder getInstance() {
        return instance;
    }

    public Map<String, AdaptiveHoeffdingTreeModel> getHoeffdingModelMap() {
        return hoeffdingModelMap;
    }

    public Map<String, AdaptiveHoeffdingTreeModel> getClonedHoeffdingModelMap() {
        Map<String, AdaptiveHoeffdingTreeModel> clonedMap = new HashMap<>();
        for (Map.Entry<String, AdaptiveHoeffdingTreeModel> entry : hoeffdingModelMap.entrySet()) {
            clonedMap.put(entry.getKey(), new AdaptiveHoeffdingTreeModel(entry.getValue()));
        }
        return clonedMap;
    }

    public void setHoeffdingModelMap(Map<String, AdaptiveHoeffdingTreeModel> modelsMap) {
        this.hoeffdingModelMap = modelsMap;
    }

    public AdaptiveHoeffdingTreeModel getHoeffdingModel(String name) {
        return hoeffdingModelMap.get(name);
    }

    public void deleteHoeffdingModel(String name) {
        hoeffdingModelMap.remove(name);
    }

    public void addHoeffdingModel(String name, AdaptiveHoeffdingTreeModel model) {
        hoeffdingModelMap.put(name, model);
    }
}
