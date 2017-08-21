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
package org.wso2.extension.siddhi.execution.ml.util;

/**
 * Special mathematical functions used in the ML algorithms.
 */
public class MathUtil {
    /**
     * Dot product of two 'double' vectors.
     * @param vector1 vector 1
     * @param vector2 vector 2
     * @return the dot product.
     */
    public static double dot(double[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("The dimensions have to be equal!");
        }

        double sum = 0;
        for (int i = 0; i < vector1.length; i++) {
            sum += vector1[i] * vector2[i];
        }

        return sum;
    }

    /**
     * finds the euclidean distance between two input points of equal dimension
     * @param point1 input point one
     * @param point2 input point two
     * @return euclidean distance between point1 and point2
     */
    public static double euclideanDistance(double[] point1, double[] point2) {
        double sum = 0.0;
        int dimensionality = point1.length;
        for (int i=0; i<dimensionality; i++) {
            sum += Math.pow((point1[i] - point2[i]),2);
        }
        double dist = Math.sqrt(sum);
        dist = Math.round(dist * 10000.0) / 10000.0;
        return dist;
    }

}
