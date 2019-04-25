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
package org.wso2.extension.siddhi.execution.streamingml.bayesian.regression;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class BayesianRegressionStreamProcessorExtensionTestCase {

    private static final Logger logger = Logger.getLogger(BayesianRegressionStreamProcessorExtensionTestCase.class);
    private AtomicInteger count;
    private String trainingStream = "@App:name('BayesianRegressionTestApp') " +
            "\ndefine stream StreamTrain (attribute_0 double, " +
            "attribute_1 double, attribute_2 " + "double, attribute_3 double, attribute_4 double );";

    private String trainingQuery = ("@info(name = 'query-train') from " +
            "StreamTrain#streamingml:updateBayesianRegression" + "('model1', attribute_4, 0.1, attribute_0, " +
            "attribute_1, attribute_2, attribute_3) \n" + "insert all events into trainOutputStream;\n");


    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }


    @Test
    public void testBayesianRegressionStreamProcessorExtension1() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - Assert predictions and evolution");

        SiddhiManager siddhiManager = new SiddhiManager();

        String trainingQuery = ("@info(name = 'query-train') from " +
                "StreamTrain#streamingml:updateBayesianRegression" + "('ml', attribute_4, 'nadam', 0.01, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into trainOutputStream;\n");

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, " +
                "attribute_2 double, attribute_3 double);";

        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('ml', " +
                " attribute_0, attribute_1, attribute_2, attribute_3) " +
                "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidence " +
                "insert into outputStream;");


        int nSamples, nDimensions, trainSamples;
        INDArray data, targets, w;
        nSamples = 1000;
        nDimensions = 4;
        trainSamples = 800;

        int[] trainSplit = java.util.stream.IntStream.rangeClosed(0, trainSamples).toArray();
        int[] testSplit = java.util.stream.IntStream.rangeClosed(trainSamples, nSamples - 1).toArray();

        data = Nd4j.rand(new int[]{nSamples, nDimensions}, -1, 1,
                Nd4j.getRandomFactory().getNewRandomInstance());
        w = Nd4j.create(new double[]{0.8, -1.2, 2.5, -0.8, 3.3}, new int[]{nDimensions, 1});
        targets = data.mmul(w);

        double[][] trainData, testData;
        double[] trainTargets, testTargets;
        Double[] predictedTargets, predictiveConfidence;

        trainData = data.getRows(trainSplit).toDoubleMatrix();
        trainTargets = targets.getRows(trainSplit).toDoubleVector();

        testData = data.getRows(testSplit).toDoubleMatrix();
        testTargets = targets.getRows(testSplit).toDoubleVector();

        predictedTargets = new Double[nSamples - trainSamples];
        predictiveConfidence = new Double[nSamples - trainSamples];

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    trainingStream + inStreamDefinition + trainingQuery + query);

            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(inEvents);
                    if (count.get() < (testData.length)) {
                        predictedTargets[count.get()] =
                                (Double) inEvents[0].getData()[nDimensions];
                        predictiveConfidence[count.get()] =
                                (Double) inEvents[0].getData()[nDimensions + 1];
                    }
                    count.incrementAndGet();
                }
            });

            try {
                InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamTrain");
                siddhiAppRuntime.start();

                Object[] event = new Object[nDimensions + 1];
                for (int i = 0; i < trainSamples; i++) {
                    for (int j = 0; j < nDimensions; j++) {
                        event[j] = trainData[i][j];
                    }
                    event[nDimensions] = trainTargets[i];
                    inputHandler.send(event);
                }

                Thread.sleep(2000);

                InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("StreamA");
                // send some unseen data for prediction
                event = new Object[nDimensions];
                for (int i = 0; i < (testData.length); i++) {
                    for (int j = 0; j < nDimensions; j++) {
                        event[j] = testData[i][j];
                    }
                    inputHandler1.send(event);
                }
                SiddhiTestHelper.waitForEvents(200, nDimensions - trainSamples, count, 60000);

                double mse = 0.0;
                for (int i = 0; i < testData.length; i++) {
                    mse += Math.pow(testTargets[i] - predictedTargets[i], 2);
                }

                mse = mse / testData.length;

                AssertJUnit.assertEquals(mse, 0.0, 0.1);
                logger.info("Model successfully trained with mean squared error: " + mse);


            } catch (Exception e) {
                logger.error(e.getCause().getMessage());
                AssertJUnit.fail("Model fails build");

            } finally {
                siddhiAppRuntime.shutdown();
            }
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.fail("Model fails build");
        } finally {
            siddhiManager.shutdown();
        }

    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension2() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - Features are not of type double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('model1', " + "10," +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("model.features in 6th parameter is not "
                    + "a numerical type attribute. Found BOOL. Check the input stream definition"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension3() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - Number of prediction samples is not int ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('model1', " + "0.5, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter type found for the " +
                    "prediction.samples argument. Expected: INT but found: DOUBLE"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension4() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - Number of samples less than 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('model1', " + "0," +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter value found for the " +
                    "prediction.samples argument. Expected a value greater than zero, but found: 0"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension5() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression(attribute_4, " + "1000," +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Parameter model.name must be a constant but "
                    + "found io.siddhi.core.executor.VariableExpressionExecutor"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension6() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression() \n" + "insert all " +
                "events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid number of parameters [0] for " +
                    "streamingml:bayesianRegression"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension7() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - Incompatible model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('model1', " + "100, " +
                "attribute_0, attribute_1, attribute_2) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream +
                    inStreamDefinition + trainingQuery + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Model [model1] expects 4 features, but the " +
                    "streamingml:bayesianRegression specifies 3 features"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension8() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression(1000, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter type found for the " +
                    "model.name argument, required STRING but found INT"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension9() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('m1', " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2000) \n" +
                "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("6th parameter is not an attribute "
                    + "(VariableExpressionExecutor) present in the stream definition. Found a "
                    + "io.siddhi.core.executor.ConstantExpressionExecutor"
            ));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension10() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('m1', " + "10000, " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid number of parameters for " +
                    "streamingml:bayesianRegression. This Stream Processor requires at most 6 parameters, " +
                    "namely, model.name, prediction.samples[optional], model.features but found 7 parameters"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension11() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - init predict first and then "
                + "update model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('model1', attribute_0, "
                + "attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream
                    + inStreamDefinition + query + trainingQuery);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Model [model1.BayesianRegressionTestApp] "
                    + "needs to initialized prior to be used with streamingml:bayesianRegression. Perform "
                    + "streamingml:updateBayesianRegression process first"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension12() {
        logger.info("BayesianRegressionStreamProcessorExtension TestCase - model is visible only within the " +
                "SiddhiApp");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('BayesianRegressionTestApp2') \ndefine stream StreamA " +
                "(attribute_0 double, " +
                "attribute_1 double, attribute_2 double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianRegression('model1', " +
                "1000, attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into " + "outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(trainingStream + trainingQuery);
            // should be successful even though both the apps are using the same model name with different feature
            // values
            SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e.getCause().getMessage().contains(
                    "Model [model1.BayesianRegressionTestApp2] needs to initialized prior to be " +
                            "used with streamingml:bayesianRegression. " +
                            "Perform streamingml:updateBayesianRegression process first."));
        } finally {
            siddhiManager.shutdown();
        }
    }


}
