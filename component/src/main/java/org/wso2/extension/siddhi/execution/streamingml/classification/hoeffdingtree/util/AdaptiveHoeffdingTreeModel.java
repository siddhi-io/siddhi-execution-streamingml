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

import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.classifiers.trees.HoeffdingAdaptiveTree;
import moa.core.FastVector;
import moa.core.ObjectRepository;
import moa.options.AbstractOptionHandler;
import moa.streams.InstanceStream;
import moa.tasks.TaskMonitor;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.streamingml.util.CoreUtils;
import org.wso2.extension.siddhi.execution.streamingml.util.MathUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the Hoeffding Adaptive Tree Model
 */
public class AdaptiveHoeffdingTreeModel extends AbstractOptionHandler {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(AdaptiveHoeffdingTreeModel.class);

    private InstancesHeader streamHeader;
    private int numberOfAttributes;
    private HoeffdingAdaptiveTree hoeffdingAdaptiveTree;
    private List<String> classes = new ArrayList<String>();

    public AdaptiveHoeffdingTreeModel() {
    }

    public AdaptiveHoeffdingTreeModel(AdaptiveHoeffdingTreeModel model) {
        this.streamHeader = model.streamHeader;
        this.numberOfAttributes = model.numberOfAttributes;
        this.hoeffdingAdaptiveTree = model.hoeffdingAdaptiveTree;
        this.classes = model.classes;
    }

    @Override
    public void getDescription(StringBuilder stringBuilder, int i) {
        logger.info("Hoeffding AdaptiveTree Model with ADWIN concept drift detection");
    }


    /**
     * Initialize the model with input stream definition.
     *
     * @param numberOfAttributes number of feature attributes
     */
    public void init(int numberOfAttributes) {
        hoeffdingAdaptiveTree = new HoeffdingAdaptiveTree();
        this.numberOfAttributes = numberOfAttributes;
        streamHeader = setStreamHeader(this.numberOfAttributes);
        hoeffdingAdaptiveTree.setModelContext(streamHeader);
        hoeffdingAdaptiveTree.prepareForUse();
    }


    /**
     * @param gracePeriod            number of instances a leaf should observe between split attempts.
     * @param splittingCriteria      Split criterion to use. 0:InfoGainSplitCriterion, 1:GiniSplitCriterion".
     * @param allowableSplitError    Allowable error in split decision, values closer to 0 will take.
     * @param breakTieThreshold      Threshold below which a split will be forced to break ties.
     * @param binarySplitOption      Allow binary splits.
     * @param disablePrePruning      Disable pre-pruning.
     * @param leafpredictionStrategy Leaf prediction to use.
     */
    public void setConfigurations(int gracePeriod, int splittingCriteria,
                                  double allowableSplitError, double breakTieThreshold,
                                  boolean binarySplitOption, boolean disablePrePruning,
                                  int leafpredictionStrategy) {
        hoeffdingAdaptiveTree.gracePeriodOption.setValue(gracePeriod);
        if (splittingCriteria == 0) {
            hoeffdingAdaptiveTree.splitCriterionOption
                    .setValueViaCLIString("InfoGainSplitCriterion");
        } else {
            hoeffdingAdaptiveTree.splitCriterionOption
                    .setValueViaCLIString("GiniSplitCriterion");
        }
        hoeffdingAdaptiveTree.splitConfidenceOption.setValue(allowableSplitError);
        hoeffdingAdaptiveTree.tieThresholdOption.setValue(breakTieThreshold);
        hoeffdingAdaptiveTree.binarySplitsOption.setValue(binarySplitOption);
        hoeffdingAdaptiveTree.noPrePruneOption.setValue(disablePrePruning);
        hoeffdingAdaptiveTree.leafpredictionOption.setChosenIndex(leafpredictionStrategy);

    }


    /**
     * @param cepEvent event data
     * @param classes  class  label list
     * @return
     */
    public void trainOnEvent(double[] cepEvent, List<String> classes) {
        this.classes = classes;
        Instance trainInstance = createInstance(cepEvent);
        trainInstance.setClassValue(cepEvent[numberOfAttributes - 1]);
        //training on the event instance
        hoeffdingAdaptiveTree.trainOnInstanceImpl(trainInstance);
    }


    /**
     * @param modelEvaluation Prequential Model Evaluator.
     * @param cepEvent        Event data.
     * @param classes         Class label list.
     * @return
     */
    public double testThenTrainOnEvent(PrequentialModelEvaluation modelEvaluation,
                                       double[] cepEvent, List<String> classes) {
        this.classes = classes;
        Instance instance = createInstance(cepEvent);
        instance.setClassValue(cepEvent[numberOfAttributes - 1]);
        double[] votes = hoeffdingAdaptiveTree.getVotesForInstance(instance);
        hoeffdingAdaptiveTree.trainOnInstanceImpl(instance);
        modelEvaluation.addResult(instance, votes);
        return modelEvaluation.getFractionCorrectlyClassified();
    }


    /**
     * @param cepEvent Event data.
     * @return predicted class index, probability of the prediction.
     */
    public Object[] getPrediction(double[] cepEvent) {
        Instance testInstance = createInstance(cepEvent);
        double[] votes = hoeffdingAdaptiveTree.getVotesForInstance(testInstance);
        int classIndex = CoreUtils.argMaxIndex(votes);
        double confidenceLevel = getPredictionConfidence(votes);
        return new Object[]{classIndex, confidenceLevel};
    }

    /**
     * @param cepEvent Event Data
     * @return represents a single Event
     */
    private Instance createInstance(double[] cepEvent) {
        Instance instance = new DenseInstance(1.0D, cepEvent);
        //set schema header for the instance
        instance.setDataset(streamHeader);
        return instance;
    }


    /**
     * @param numberOfAttributes
     * @return represents stream definition
     */
    private InstancesHeader setStreamHeader(int numberOfAttributes) {
        FastVector headerAttributes = new FastVector();
        for (int i = 0; i < numberOfAttributes - 1; i++) {
            headerAttributes.addElement(
                    new Attribute("att_" + i));
        }
        InstancesHeader streamHeader = new InstancesHeader(new Instances
                (this.getCLICreationString(InstanceStream.class), headerAttributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes());
        return streamHeader;
    }

    /**
     * @return
     */
    public InstancesHeader getStreamHeader() {
        return this.streamHeader;
    }

    /**
     * @return
     */
    public List<String> getClasses() {
        return classes;
    }

    /**
     * @param votes-Vote prediction for the class labels
     * @return
     */
    private double getPredictionConfidence(double[] votes) {
        return (CoreUtils.argMax(votes) / MathUtil.sum(votes));

    }

    /**
     * @return
     */
    public int getNumberOfAttributes() {
        return numberOfAttributes;
    }


    @Override
    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {

    }
}

