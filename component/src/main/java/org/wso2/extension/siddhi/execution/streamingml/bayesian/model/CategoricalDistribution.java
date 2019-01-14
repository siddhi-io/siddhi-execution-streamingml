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
package org.wso2.extension.siddhi.execution.streamingml.bayesian.model;

import org.nd4j.autodiff.samediff.SDVariable;
import org.nd4j.autodiff.samediff.SameDiff;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;

/**
 * implements Categorical distribution.
 */
public class CategoricalDistribution extends Distribution {

    private static final long serialVersionUID = 1417771042959223863L;
    private SDVariable prob;

    /**
     * Construct the categorical distribution.
     *
     * @param logits should be 2-dimensional.
     *               the dimensions should follow the order (input_size, num_classes)
     * @param sd     SameDiff context
     */
    public CategoricalDistribution(SDVariable logits, SameDiff sd) {
        this.prob = sd.softmax(logits);
        this.sd = sd;
    }

    /**
     * categorical log probability is implemented based on softmax-crossentropy
     * the implementation is based on following formula
     * <p>
     * log(p(y)) = sum[1:num_classes]{log(softmax[logits])*y}
     * <p>
     * however, log(softmax(logits)) can be infinity for some case.
     * hence, unecessary log computations are avoided using transformed formula
     * <p>
     * log(p(y)) = sum[1:num_classes]{log(softmax[logits]*y)}
     * <p>
     * output of the both formulas are equivalent if y is one-hot encoded
     *
     * @param values one-hot encoded labels
     * @return log probability as a SameDiff var
     */
    @Override
    public SDVariable logProbability(SDVariable values) {
        return sd.log(prob.mul(values).sum(1));
    }

    @Override
    public SDVariable sample() {
        return null;
    }

    @Override
    public SDVariable sample(int n) {
        return null;
    }

    @Override
    public SDVariable klDivergence(Distribution distribution) throws SiddhiAppCreationException {
        throw new SiddhiAppCreationException("kl-divergence is not implemented for categorical distribution");
    }

    public SDVariable getProb() {
        return prob;
    }
}
