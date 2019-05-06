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
package io.siddhi.extension.execution.streamingml.bayesian.regression.util;

import io.siddhi.extension.execution.streamingml.bayesian.util.LinearRegression;

import java.util.HashMap;
import java.util.Map;

/**
 * Data holder which keeps the instances of @{@link LinearRegression}.
 */
public class LinearRegressionModelHolder {
    private static final LinearRegressionModelHolder instance = new LinearRegressionModelHolder();

    /**
     * Key - name of the model.
     * Value - @{@link LinearRegression}
     */
    private Map<String, LinearRegression> linearRegressionMap;

    private LinearRegressionModelHolder() {
        linearRegressionMap = new HashMap();
    }

    public static LinearRegressionModelHolder getInstance() {
        return instance;
    }

    public Map<String, LinearRegression> getLinearRegressionMap() {
        return linearRegressionMap;
    }

    public void setLinearRegressionMap(Map<String, LinearRegression> modelsMap) {
        this.linearRegressionMap = modelsMap;
    }

    public LinearRegression getLinearRegressionModel(String name) {
        return linearRegressionMap.get(name);
    }

    public void deleteLinearRegressionModel(String name) {
        linearRegressionMap.remove(name);
    }

    public void addLinearRegressionModel(String name, LinearRegression model) {
        linearRegressionMap.put(name, model);
    }

    public LinearRegression getClonedLinearRegressionModel(String modelName) {
        return new LinearRegression(linearRegressionMap.get(modelName));
    }

}
