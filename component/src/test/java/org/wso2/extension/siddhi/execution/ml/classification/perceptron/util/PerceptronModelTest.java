package org.wso2.extension.siddhi.execution.ml.classification.perceptron.util;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class PerceptronModelTest {

    @Test
    public void testPerceptron() {
        PerceptronModel model = new PerceptronModel();
        model.update(true, new double[]{1.0, 1.0});
        model.update(true, new double[]{0.9, 0.89});
        model.update(false, new double[]{0.0, 0.0});
        model.update(false, new double[]{0.01, 0.4});
        model.update(true, new double[]{0.80, 0.81});
        model.update(true, new double[]{0.93, 0.71});
        model.update(false, new double[]{0.02, 0.30});
        model.update(false, new double[]{0.29, 0.24});

        AssertJUnit.assertEquals(false, model.classify(new double[]{0.0, 0.24})[0]);
        AssertJUnit.assertEquals(false, model.classify(new double[]{0.32, 0.40})[0]);
        AssertJUnit.assertEquals(true, model.classify(new double[]{0.72, 0.85})[0]);
    }

    @Test
    public void testPerceptron4Dimensions() {
        // Perceptron will work only with linearly separable datasets
        PerceptronModel model = new PerceptronModel();
        model.update(true, new double[]{1.0, 1.0, 0.2, 0.13});
        model.update(true, new double[]{0.9, 0.89, 0.3, 0.02});
        model.update(false, new double[]{0.0, 0.0, 1.0, 0.82});
        model.update(false, new double[]{0.01, 0.4, 0.77, 0.92});
        model.update(true, new double[]{0.80, 0.81, 0.11, 0.13});
        model.update(false, new double[]{0.02, 0.30, 0.88, 0.76});
        model.update(true, new double[]{0.93, 0.71, 0.02, 0.122});
        model.update(false, new double[]{0.29, 0.24, 0.98, 0.65});

        AssertJUnit.assertEquals(false, model.classify(new double[]
                {0.0, 0.0, 0.90, 0.62})[0]);
        AssertJUnit.assertEquals(false, model.classify(new double[]
                {0.0, 0.0, 0.77, 1.0})[0]);
        AssertJUnit.assertEquals(true, model.classify(new double[]
                {0.990, 0.807, 0.12, 0.15})[0]);
    }
}
