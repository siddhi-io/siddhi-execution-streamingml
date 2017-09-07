package org.wso2.extension.siddhi.execution.streamingml.util;

import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

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
     * @param inputDefinition
     * @param attributeExpressionExecutors
     * @param startIndex                   starting index
     * @param noOfFeatures
     * @return
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


    private static boolean isNumeric(Attribute.Type attributeType) {
        return numericTypes.contains(attributeType);
    }

    public static boolean isLabelType(Attribute.Type attributeType) {
        return labelTypes.contains(attributeType);
    }


    /**
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
