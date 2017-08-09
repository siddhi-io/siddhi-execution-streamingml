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
package org.wso2.extension.siddhi.execution.ml.classification.perceptron.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Data holder which keeps the instances of @{@link PerceptronModel}
 */
public class PerceptronModelsHolder {

    private static final PerceptronModelsHolder instance = new PerceptronModelsHolder();

    /**
     * Key - name of the model
     * Value - @{@link PerceptronModel}
     */
    private Map<String, PerceptronModel> perceptronModelMap = new HashMap();

    private PerceptronModelsHolder() {
    }

    public static PerceptronModelsHolder getInstance() {
        return instance;
    }

    public Map<String, PerceptronModel> getPerceptronModelMap() {
        return perceptronModelMap;
    }

    public Map<String, PerceptronModel> getClonedPerceptronModelMap() {
        Map<String, PerceptronModel> clonedMap = new HashMap<>();
        for (Map.Entry<String, PerceptronModel> entry : perceptronModelMap.entrySet()) {
            clonedMap.put(entry.getKey(), new PerceptronModel(entry.getValue()));
        }
        return clonedMap;
    }

    public void setPerceptronModelMap(Map<String, PerceptronModel> modelsMap) {
        this.perceptronModelMap = modelsMap;
    }

    public PerceptronModel getPerceptronModel(String name) {
        return perceptronModelMap.get(name);
    }

    public void addPerceptronModel(String name, PerceptronModel model) {
        perceptronModelMap.put(name, model);
    }

}
