package org.wso2.extension.siddhi.execution.streamingml.util;

import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.List;

/**
 * Common utils for Streaming Machine Learning tasks.
 */
public class CoreUtils {
    /**
     * Index of the Maximum from double array
     *
     * @param doubles
     * @return index of the maximum
     */
    public static int argMaxIndex(double[] doubles) {
        double maximum = 0.0D;
        int maxIndex = 0;
        for (int i = 0; i < doubles.length; ++i) {
            if (i == 0 || doubles[i] > maximum) {
                maxIndex = i;
                maximum = doubles[i];
            }
        }
        return maxIndex;
    }

    /**
     * Maximum from the double array
     * @param doubles
     * @return
     */
    public static double argMax(double[] doubles) {
        double maximum = 0.0D;
        for (int i = 0; i < doubles.length; ++i) {
            if (i == 0 || doubles[i] > maximum) {
                maximum = doubles[i];
            }
        }
        return maximum;
    }


    /**
     * @param inputDefinition
     * @param attributeExpressionExecutors
     * @param startIndex
     * @return
     */
    public static List<VariableExpressionExecutor> extractAndValidateFeatures(
            AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors,
            int startIndex, int endIndex) {

        List<Attribute.Type> numericTypes = new ArrayList<>();
        numericTypes.add(Attribute.Type.DOUBLE);
        numericTypes.add(Attribute.Type.INT);
        numericTypes.add(Attribute.Type.FLOAT);
        numericTypes.add(Attribute.Type.LONG);

        List<VariableExpressionExecutor> featureVariableExpressionExecutors = new ArrayList<>();

        // feature values start
        for (int i = startIndex; i < endIndex; i++) {
            if (attributeExpressionExecutors[i] instanceof VariableExpressionExecutor) {
                featureVariableExpressionExecutors.add((VariableExpressionExecutor)
                        attributeExpressionExecutors[i]);
                // other attributes should be numeric type.
                String attributeName = ((VariableExpressionExecutor)
                        attributeExpressionExecutors[i]).getAttribute().getName();
                Attribute.Type featureAttributeType = inputDefinition.
                        getAttributeType(attributeName);

                //feature attributes not numerical type
                if (!(numericTypes.contains(featureAttributeType))) {
                    throw new SiddhiAppValidationException("model.features in " + (i + 1) + "th parameter is not " +
                            "a numerical type attribute. Found " +
                            attributeExpressionExecutors[i].getReturnType()
                            + ". Check the input stream definition.");
                }

            } else {
                throw new SiddhiAppValidationException((i + 1) + "th parameter is not " +
                        "an attribute (VariableExpressionExecutor). Check the number of " +
                        "attribute entered as a attribute set with number " +
                        "of attribute configuration parameter");
            }
        }
        return featureVariableExpressionExecutors;
    }


    /**
     *
     * @param inputDefinition
     * @param attributeExpressionExecutors
     * @param classIndex
     * @return
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
            if (classLabelAttributeType == Attribute.Type.STRING || classLabelAttributeType == Attribute.Type.BOOL) {
                classLabelVariableExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[classIndex];
            } else {
                throw new SiddhiAppValidationException(String.format("[label attribute] in %s th index of " +
                                "classifierUpdate should be either a %s or a %s but found %s",
                        classIndex, Attribute.Type.BOOL, Attribute
                                .Type.STRING, classLabelAttributeType));
            }

        } else {
            throw new SiddhiAppValidationException((classIndex) + "th parameter is not " +
                    "an attribute (VariableExpressionExecutor). Check the number of " +
                    "attribute entered as a attribute set with number "
                    + "of attribute configuration parameter");
        }

        return classLabelVariableExecutor;

    }
}
