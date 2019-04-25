package org.wso2.extension.siddhi.execution.streamingml.util;

import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Common utils for Streaming Machine Learning tasks.
 */
public class CoreUtils {
    private static final List<Attribute.Type> numericTypes = Arrays.asList(Attribute.Type.INT,
            Attribute.Type.DOUBLE, Attribute.Type.LONG, Attribute.Type.FLOAT);
    private static final List<Attribute.Type> labelTypes = Arrays.asList(Attribute.Type.STRING, Attribute.Type.BOOL);

    /**
     * Validate and extract feature attribute executors
     * @param inputDefinition the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param startIndex starting index of the feature attributes
     * @param noOfFeatures number of feature attributes
     * @return list of VariableExpressionExecutors
     */
    public static List<VariableExpressionExecutor> extractAndValidateFeatures(
            AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors,
            int startIndex, int noOfFeatures) {

        List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();

        // feature values start
        for (int i = startIndex; i < (startIndex + noOfFeatures); i++) {
            if (attributeExpressionExecutors[i] instanceof VariableExpressionExecutor) {
                featureVariableExpressionExecutors.add((VariableExpressionExecutor)
                        attributeExpressionExecutors[i]);
                // other attributes should be numeric type.
                String attributeName = ((VariableExpressionExecutor)
                        attributeExpressionExecutors[i]).getAttribute().getName();
                Attribute.Type featureAttributeType = inputDefinition.
                        getAttributeType(attributeName);

                //feature attributes not numerical type
                if (!isNumeric(featureAttributeType)) {
                    throw new SiddhiAppValidationException("model.features in " + (i + 1) + "th parameter is not " +
                            "a numerical type attribute. Found " +
                            attributeExpressionExecutors[i].getReturnType()
                            + ". Check the input stream definition.");
                }

            } else {
                throw new SiddhiAppValidationException((i + 1) + "th parameter is not " +
                        "an attribute (VariableExpressionExecutor) present in the stream definition. Found a "
                        + attributeExpressionExecutors[i].getClass().getCanonicalName());
            }
        }
        return featureVariableExpressionExecutors;
    }

    /**
     * Check feature attribute to be a numeric
     * @param attributeType feature attribute type
     * @return true/false
     */
    public static boolean isNumeric(Attribute.Type attributeType) {
        return numericTypes.contains(attributeType);
    }

    /**
     * Check label attribute to be a valid type
     * @param attributeType label feature attribute type
     * @return true/false
     */
    public static boolean isLabelType(Attribute.Type attributeType) {
        return labelTypes.contains(attributeType);
    }

    /**
     * Validate and extract Class label executor
     * @param inputDefinition the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param classIndex index of the class label attribute
     * @return class label VariableExpressionExecutor
     */
    public static VariableExpressionExecutor extractAndValidateClassLabel
    (AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, int classIndex) {
        VariableExpressionExecutor classLabelVariableExecutor;

        if (attributeExpressionExecutors[classIndex] instanceof VariableExpressionExecutor) {
            // other attributes should be numeric type.
            String attributeName = ((VariableExpressionExecutor)
                    attributeExpressionExecutors[classIndex]).getAttribute().getName();
            Attribute.Type classLabelAttributeType = inputDefinition.
                    getAttributeType(attributeName);

            //class label should be String or Bool
            if (isLabelType(classLabelAttributeType)) {
                classLabelVariableExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[classIndex];
            } else {
                throw new SiddhiAppValidationException(String.format("[label attribute] in %s th index of " +
                                "classifierUpdate should be either a %s or a %s but found %s",
                        classIndex, Attribute.Type.BOOL, Attribute
                                .Type.STRING, classLabelAttributeType));
            }

        } else {
            throw new SiddhiAppValidationException((classIndex) + "th parameter is not " +
                    "an attribute (VariableExpressionExecutor) present in the stream definition. Found a "
                    + attributeExpressionExecutors[classIndex].getClass().getCanonicalName());
        }
        return classLabelVariableExecutor;
    }
}
