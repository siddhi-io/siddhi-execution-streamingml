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
package io.siddhi.extension.execution.streamingml.bayesian.classification;

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

import static org.nd4j.linalg.ops.transforms.Transforms.sigmoid;

public class BayesianClassificationStreamProcessorExtensionTestCase {

    private static final Logger logger = Logger.getLogger(BayesianClassificationStreamProcessorExtensionTestCase.class);
    private AtomicInteger count;
    private String trainingStream = "@App:name('BayesianClassificationTestApp') " +
            "\ndefine stream StreamTrain (attribute_0 double, " +
            "attribute_1 double, attribute_2  double, attribute_3 double, attribute_4 string );";

    private String trainingQuery = ("@info(name = 'query-train') from " +
            "StreamTrain#streamingml:updateBayesianClassification" + "('ml', 3, attribute_4, 'nadam', 0.01, " +
            "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
            "insert all events into trainOutputStream;\n");


    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension1() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase " +
                "- Assert predictions and evolution");
        SiddhiManager siddhiManager = new SiddhiManager();

        String trainingQuery = ("@info(name = 'query-train') from " +
                "StreamTrain#streamingml:updateBayesianClassification" + "('ml', 2, attribute_4, 'nadam', 0.01, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into trainOutputStream;\n");

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, " +
                "attribute_2 double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('ml', " +
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
        targets = sigmoid(data.mmul(w)).gt(0.5);


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
                                Double.parseDouble((String) inEvents[0].getData()[nDimensions]);
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

                double nCorrect = 0.0;
                double accuracy;
                for (int i = 0; i < testData.length; i++) {
                    if (testTargets[i] == predictedTargets[i]) {
                        nCorrect += 1;
                    }
                }
                accuracy = nCorrect / testData.length;

                AssertJUnit.assertEquals(accuracy, 0.95, 0.05);
                logger.info("Model successfully trained with accuracy: " + accuracy);


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

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianClassificationStreamProcessorExtension2() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - Features are not of type numeric");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('ml', " + "10," +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianClassificationStreamProcessorExtension3() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - " +
                "Number of prediction samples is not int ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('ml', " + "0.5, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension4() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - Number of samples less than 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('ml', " + "0," +
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

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianClassificationStreamProcessorExtension5() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - model name is not string");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification(34," +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianClassificationStreamProcessorExtension6() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification(attribute_4, "
                + "1000, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianClassificationStreamProcessorExtension7() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification() \n" + "insert all " +
                "events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension8() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - Incompatible model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('ml', " + "100, " +
                "attribute_0, attribute_1, attribute_2) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream +
                    inStreamDefinition + trainingQuery + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Model [ml] expects 4 features, but the " +
                    "streamingml:bayesianClassification specifies 3 features"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianClassificationStreamProcessorExtension9() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification(1000, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension10() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('m1', " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2000) \n" +
                "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail("Model fails build");
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
    public void testBayesianClassificationStreamProcessorExtension11() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('m1', " + "10000, " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail("Model fails build");
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid number of parameters for " +
                    "streamingml:bayesianClassification. This Stream Processor requires at most 6 parameters, " +
                    "namely, model.name, prediction.samples[optional], model.features but found 7 parameters"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension12() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - init predict first and then "
                + "update model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('ml', attribute_0, "
                + "attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream
                    + inStreamDefinition + query + trainingQuery);
            AssertJUnit.fail("Model fails build");
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Model [ml.BayesianClassificationTestApp] "
                    + "needs to initialized prior to be used with streamingml:bayesianClassification. Perform "
                    + "streamingml:updateBayesianClassification process first"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension13() {
        logger.info("BayesianClassificationStreamProcessorExtension TestCase - model is visible only within the " +
                "SiddhiApp");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('BayesianClassificationTestApp2') \ndefine stream StreamA " +
                "(attribute_0 double, " +
                "attribute_1 double, attribute_2 double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:bayesianClassification('ml', " +
                "1000, attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into " + "outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(trainingStream + trainingQuery);
            // should be successful even though both the apps are using the same model name with different feature
            // values
            SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail("Model fails build");
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e.getCause().getMessage().contains(
                    "Model [ml.BayesianClassificationTestApp2] needs to initialized prior to be " +
                            "used with streamingml:bayesianClassification. " +
                            "Perform streamingml:updateBayesianClassification process first."));
        } finally {
            siddhiManager.shutdown();
        }
    }


}
