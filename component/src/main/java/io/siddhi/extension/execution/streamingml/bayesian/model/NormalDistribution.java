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
package io.siddhi.extension.execution.streamingml.bayesian.model;

import io.siddhi.core.exception.SiddhiAppCreationException;
import org.nd4j.autodiff.samediff.SDVariable;
import org.nd4j.autodiff.samediff.SameDiff;
import org.nd4j.linalg.api.ndarray.INDArray;

/**
 * implements Gaussian distribution.
 * <p>
 * sampling uses reparameterization trick [1] to reduce the variance of the gradients
 * reparameterization trick follows
 * q(z) = loc + scale * epsilon, here epsilon ~ N(0, 1)
 * <p>
 * [1] https://arxiv.org/abs/1312.6114
 */
public class NormalDistribution extends Distribution {

    private static final long serialVersionUID = 6666442004853927088L;
    private SDVariable loc;
    private SDVariable scale;

    /**
     * constructs a normal distribution.
     *
     * @param loc   location/mean. Expected a 2-dimensional variable s,t (input_size, output_size)
     * @param scale standard deviation
     * @param sd    SameDiff context
     */
    public NormalDistribution(SDVariable loc, SDVariable scale, SameDiff sd) {
        this.loc = loc;
        this.scale = scale;
        this.sd = sd;
    }

    /**
     * constructs a normal distribution.
     *
     * @param loc   location/mean
     * @param scale standard deviation
     * @param sd    SameDiff context
     */
    NormalDistribution(SDVariable loc, INDArray scale, SameDiff sd) {
        this.loc = loc;
        this.scale = sd.var("scale", scale);
        this.sd = sd;

    }

    /**
     * computes the log-probability of normal distribution.
     * uses following simplified formula
     * <p>
     * log(p(x)) = -log(sqrt[2*PI]*scale)-([x-loc]/[2*scale])^2
     *
     * @param value x values
     * @return log probability SameDiff var
     */
    @Override
    public SDVariable logProbability(SDVariable value) {
        return sd.neg(sd.log(scale.mul(Math.sqrt(2 * Math.PI)))).sub(sd.square(value.sub(loc).div(scale.mul(2))));
    }


    @Override
    public SDVariable sample() {
        SDVariable epsilon = sd.randomNormal(0, 1, scale.getShape()); // auxiliary random variable
        return loc.add(scale.mul(epsilon)); // applying reparameterization trick
    }

    @Override
    public SDVariable sample(int n) {
        SDVariable epsilon = sd.randomNormal(0, 1, this.scale.getShape()[0], this.scale.getShape()[1], n);

        // reshaping 2d variables to 3d vars
        SDVariable loc = sd.reshape(this.loc, this.loc.getShape()[0], this.loc.getShape()[1], 1);
        SDVariable scale = sd.reshape(this.scale, this.scale.getShape()[0], this.scale.getShape()[1], 1);

        return loc.add(scale.mul(epsilon));
    }

    /**
     * kullback leibler divergence between two normal densities.
     * based on following expression
     * <p>
     * KL(q(loc_1,scale_1)||p(loc_2,scale_2) =
     * log(scale_2) - log(scale_1) + (scale_1^2 + [loc_1-loc_2]^2)/(2*scale_2^2) + 0.5
     *
     * @param distribution reference distribution p(x)
     * @return SameDiff variable
     * @throws SiddhiAppCreationException if distribution is not Gaussian
     */
    @Override
    public SDVariable klDivergence(Distribution distribution) throws SiddhiAppCreationException {
        if (distribution instanceof NormalDistribution) {
            SDVariable loc2, scale2;
            loc2 = ((NormalDistribution) distribution).loc;
            scale2 = ((NormalDistribution) distribution).scale;
            return sd.log(scale2.div(scale)).add(sd.square(scale).add(sd.square(loc.sub(loc2)))
                    .div(sd.square(scale2).mul(2))).sub(0.5);
        } else {
            throw new SiddhiAppCreationException("kl-divergence with normal and other distributions are not supported");
        }
    }

    public SDVariable getLoc() {
        return loc;
    }

    public SDVariable getScale() {
        return scale;
    }
}
