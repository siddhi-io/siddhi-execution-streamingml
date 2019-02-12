/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.extension.siddhi.execution.streamingml.bayesian.classification.util;

import org.wso2.extension.siddhi.execution.streamingml.bayesian.util.SoftmaxRegression;

import java.util.HashMap;
import java.util.Map;

/**
 * Data holder which keeps the instances of @{@link SoftmaxRegression}.
 */
public class SoftmaxRegressionModelHolder {
    private static final SoftmaxRegressionModelHolder instance = new SoftmaxRegressionModelHolder();

    /**
     * Key - name of the model.
     * Value - @{@link SoftmaxRegression}
     */
    private Map<String, SoftmaxRegression> softmaxRegressionMap;

    private SoftmaxRegressionModelHolder() {
        softmaxRegressionMap = new HashMap();
    }

    public static SoftmaxRegressionModelHolder getInstance() {
        return instance;
    }

    public Map<String, SoftmaxRegression> getSoftmaxRegressionMap() {
        return softmaxRegressionMap;
    }

    public void setSoftmaxRegressionMap(Map<String, SoftmaxRegression> modelsMap) {
        this.softmaxRegressionMap = modelsMap;
    }

    public SoftmaxRegression getSoftmaxRegressionModel(String name) {
        return softmaxRegressionMap.get(name);
    }

    public void deleteSoftmaxRegressionModel(String name) {
        softmaxRegressionMap.remove(name);
    }

    public void addSoftmaxRegressionModel(String name, SoftmaxRegression model) {
        softmaxRegressionMap.put(name, model);
    }

    public SoftmaxRegression getClonedSoftmaxRegressionModel(String modelName) {
        return new SoftmaxRegression(softmaxRegressionMap.get(modelName));
    }

}
