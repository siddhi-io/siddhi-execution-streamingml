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
 * implements BernoulliDistribution.
 */
public class BernoulliDistribution extends Distribution {

    private static final long serialVersionUID = 5823219877164613491L;
    private SDVariable prob;

    /**
     * Construct the bernoulli distribution.
     *
     * @param logits should be 2-dimensional. the dimensions should follow the order (input_size, 1)
     * @param sd     SameDiff context
     */
    public BernoulliDistribution(SDVariable logits, SameDiff sd) {
        this.prob = sd.sigmoid(logits);
        this.sd = sd;
    }

    @Override
    public SDVariable logProbability(SDVariable values) {
        if (values.getShape().length == 1) {
            values = sd.reshape(values, -1, 1);
        }
        return sd.log(prob.mul(values).add(prob.sub(1).mul(values.sub(1))));
    }

    @Override
    public SDVariable sample() {
        return null; //not implemented
    }

    @Override
    public SDVariable sample(int n) {
        return null; //not implemented
    }

    @Override
    public SDVariable klDivergence(Distribution distribution) throws SiddhiAppCreationException {
        throw new SiddhiAppCreationException("kl-divergence is not implemented for categorical distribution");
    }

    public SDVariable getProb() {
        return prob;
    }
}
