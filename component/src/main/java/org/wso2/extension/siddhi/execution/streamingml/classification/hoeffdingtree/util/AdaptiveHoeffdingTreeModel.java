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
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents the Hoeffding Adaptive Tree Model
 */
public class AdaptiveHoeffdingTreeModel extends AbstractOptionHandler {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(AdaptiveHoeffdingTreeModel.class);

    private String modelName;
    private InstancesHeader streamHeader;
    private int noOfFeatures;
    private int noOfClasses;
    private HoeffdingAdaptiveTree hoeffdingAdaptiveTree;
    private List<String> classes = new ArrayList<String>();


    public AdaptiveHoeffdingTreeModel(String modelName) {
        this.modelName = modelName;
    }

    public AdaptiveHoeffdingTreeModel(AdaptiveHoeffdingTreeModel model) {
        this.modelName = model.modelName;
        this.streamHeader = model.streamHeader;
        this.noOfFeatures = model.noOfFeatures;
        this.noOfClasses = model.noOfClasses;
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
     * @param noOfAttributes number of feature attributes
     * @param noOfClasses    number of classes
     */
    public void init(int noOfAttributes, int noOfClasses) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Model [%s] is being initialized.", this.modelName));
        }
        hoeffdingAdaptiveTree = new HoeffdingAdaptiveTree();
        this.noOfFeatures = noOfAttributes;
        this.noOfClasses = noOfClasses;
        streamHeader = createMOAInstanceHeader(this.noOfFeatures);
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
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Model [%s] is being manually configured.", this.modelName));
        }
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
     * @param cepEvent   event data
     * @param classLabel class  label of the cepEvent
     * @return
     */
    public void trainOnEvent(double[] cepEvent, String classLabel) {
        cepEvent[noOfFeatures - 1] = addClass(classLabel);
        Instance trainInstance = createMOAInstance(cepEvent);
        trainInstance.setClassValue(cepEvent[noOfFeatures - 1]);
        //training on the event instance
        hoeffdingAdaptiveTree.trainOnInstanceImpl(trainInstance);
    }


    /**
     * @param modelEvaluation Prequential Model Evaluator.
     * @param cepEvent        event data
     * @param classValue      class label of the cepEvent
     * @return
     */
    public double evaluationTrainOnEvent(PrequentialModelEvaluation modelEvaluation,
                                         double[] cepEvent, String classValue) {
        int classIndex = cepEvent.length - 1;

        //create instance with only the feature attributes
        double[] test = Arrays.copyOfRange(cepEvent, 0, classIndex);
        Instance testInstance = createMOAInstance(test);

        double[] votes = hoeffdingAdaptiveTree.getVotesForInstance(testInstance);

        cepEvent[classIndex] = getClasses().indexOf(classValue);
        Instance trainInstance = createMOAInstance(cepEvent);

        hoeffdingAdaptiveTree.trainOnInstanceImpl(trainInstance);
        modelEvaluation.addResult(trainInstance, votes);
        return MathUtil.roundOff(modelEvaluation.getFractionCorrectlyClassified(), 3);
    }


    /**
     * @param cepEvent Event data.
     * @return predicted class index, probability of the prediction.
     */
    public Object[] getPrediction(double[] cepEvent) {
        Instance testInstance = createMOAInstance(cepEvent);
        double[] votes = hoeffdingAdaptiveTree.getVotesForInstance(testInstance);
        int classIndex = CoreUtils.argMaxIndex(votes);
        double confidenceLevel = getPredictionConfidence(votes);
        return new Object[]{classIndex, confidenceLevel};
    }

    /**
     * @param cepEvent Event Data
     * @return represents a single Event
     */
    private Instance createMOAInstance(double[] cepEvent) {
        Instance instance = new DenseInstance(1.0D, cepEvent);
        //set schema header for the instance
        instance.setDataset(streamHeader);
        return instance;
    }


    /**
     * @param numberOfAttributes
     * @return represents stream definition
     */
    private InstancesHeader createMOAInstanceHeader(int numberOfAttributes) {
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


    public String getModelName() {
        return modelName;
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
        return this.classes;
    }

    /**
     * @param votes-Vote prediction for the class labels
     * @return
     */
    private double getPredictionConfidence(double[] votes) {
        return MathUtil.roundOff((CoreUtils.argMax(votes) / MathUtil.sum(votes)), 3);
    }

    /**
     * @return
     */
    public int getNoOfFeatures() {
        return this.noOfFeatures;
    }

    private int addClass(String label) {
        // Set class value
        if (classes.contains(label)) {
            return classes.indexOf(label);
        } else {
            if (classes.size() < noOfClasses) {
                classes.add(label);
                return classes.indexOf(label);
            } else {
                throw new SiddhiAppRuntimeException(
                        "Number of classes " + noOfClasses + " is expected from the model "
                                + modelName + " but found " + classes.size());
            }
        }

    }

    @Override
    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {

    }
}

