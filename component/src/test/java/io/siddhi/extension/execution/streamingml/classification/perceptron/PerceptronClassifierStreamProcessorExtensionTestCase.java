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

package io.siddhi.extension.execution.streamingml.classification.perceptron;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Testing @{@link PerceptronClassifierStreamProcessorExtension}
 */
public class PerceptronClassifierStreamProcessorExtensionTestCase {

    private static final Logger logger = Logger.getLogger(PerceptronClassifierStreamProcessorExtensionTestCase.class);
    private AtomicInteger count;
    private String trainingStream = "@App:name('PerceptronTestApp') \ndefine stream StreamTrain (attribute_0 double, " +
            "attribute_1 double, attribute_2 " + "double, attribute_3 double, attribute_4 string );";
    private String trainingQuery = ("@info(name = 'query-train') from " +
            "StreamTrain#streamingml:updatePerceptronClassifier" + "('model1', attribute_4, 0.1, attribute_0, " +
            "attribute_1, attribute_2, attribute_3) \n" + "insert all events into trainOutputStream;\n");


    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }

    @Test
    public void testClassificationStreamProcessorExtension1() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - Assert predictions and evolution");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0,0.5," +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                + trainingQuery + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, true, 0.5263760000000001},
                            inEvents[0].getData());
                } else if (count.get() == 2) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.33, 0.23, 0.632, 0.992, false, 0.2779144},
                            inEvents[0].getData());
                } else if (count.get() == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, false, 0.333926}, inEvents[0]
                            .getData());
                } else if (count.get() == 4) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.33, 0.23, 0.632, 0.992, true, 0.5128304},
                            inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamTrain");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{1.0, 1.0, 0.2, 0.13, "true"});
            inputHandler.send(new Object[]{0.9, 0.89, 0.3, 0.02, "true"});
            inputHandler.send(new Object[]{0.0, 0.0, 1.0, 0.82, "false"});
            inputHandler.send(new Object[]{0.01, 0.4, 0.77, 0.92, "false"});
            inputHandler.send(new Object[]{0.80, 0.81, 0.11, 0.13, "true"});
            inputHandler.send(new Object[]{0.02, 0.30, 0.88, 0.76, "false"});
            inputHandler.send(new Object[]{0.93, 0.71, 0.02, 0.122, "true"});
            inputHandler.send(new Object[]{0.29, 0.24, 0.98, 0.65, "false"});
            Thread.sleep(1100);

            InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("StreamA");
            // send some unseen data for prediction
            inputHandler1.send(new Object[]{0.8, 0.67, 0.1, 0.03});
            inputHandler1.send(new Object[]{0.33, 0.23, 0.632, 0.992});
            Thread.sleep(1000);
            // try to drift the model
            inputHandler.send(new Object[]{0.88, 1.0, 0.2, 0.13, "false"});
            inputHandler.send(new Object[]{0.9, 0.79, 0.3, 0.02, "false"});
            inputHandler.send(new Object[]{0.0, 0.0, 0.90, 0.72, "true"});
            inputHandler.send(new Object[]{0.01, 0.4, 0.87, 0.62, "true"});
            inputHandler.send(new Object[]{0.90, 0.91, 0.11, 0.13, "false"});
            inputHandler.send(new Object[]{0.02, 0.30, 0.88, 0.66, "true"});
            inputHandler.send(new Object[]{0.83, 0.79, 0.02, 0.122, "false"});
            inputHandler.send(new Object[]{0.29, 0.24, 0.98, 0.77, "true"});
            Thread.sleep(1000);
            // send some unseen data for prediction
            inputHandler1.send(new Object[]{0.8, 0.67, 0.1, 0.03});
            inputHandler1.send(new Object[]{0.33, 0.23, 0.632, 0.992});

            SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClassificationStreamProcessorExtension2() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - Features are not of type double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0,0.5," +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClassificationStreamProcessorExtension3() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - bias is not double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "2,0.5, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClassificationStreamProcessorExtension4() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - threshold is not double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0,2, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testClassificationStreamProcessorExtension5() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - threshold is greater than 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0,1.1," +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter value found for "
                    + "the model.threshold argument. Expected a value between 0 & 1, but found: 1.1"));
        }
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClassificationStreamProcessorExtension6() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier(attribute_4, " + "0.0," +
                "1.1, attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClassificationStreamProcessorExtension7() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier() \n" + "insert all " +
                "events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testClassificationStreamProcessorExtension8() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - default threshold");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                + trainingQuery + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                // should emit probabilities
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, true, 0.5263760000000001},
                            inEvents[0].getData());
                } else if (count.get() == 2) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.33, 0.23, 0.632, 0.992, false, 0.2779144},
                            inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamTrain");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{1.0, 1.0, 0.2, 0.13, "true"});
            inputHandler.send(new Object[]{0.9, 0.89, 0.3, 0.02, "true"});
            inputHandler.send(new Object[]{0.0, 0.0, 1.0, 0.82, "false"});
            inputHandler.send(new Object[]{0.01, 0.4, 0.77, 0.92, "false"});
            inputHandler.send(new Object[]{0.80, 0.81, 0.11, 0.13, "true"});
            inputHandler.send(new Object[]{0.02, 0.30, 0.88, 0.76, "false"});
            inputHandler.send(new Object[]{0.93, 0.71, 0.02, 0.122, "true"});
            inputHandler.send(new Object[]{0.29, 0.24, 0.98, 0.65, "false"});
            Thread.sleep(5000);

            InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("StreamA");
            // send some unseen data for prediction
            inputHandler1.send(new Object[]{0.8, 0.67, 0.1, 0.03});
            inputHandler1.send(new Object[]{0.33, 0.23, 0.632, 0.992});

            SiddhiTestHelper.waitForEvents(200, 2, count, 60000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension9() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - Incompatible model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0, " +
                "attribute_0, attribute_1, attribute_2) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream +
                    inStreamDefinition + trainingQuery + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Model [model1] expects 4 features, but the " +
                    "streamingml:perceptronClassifier specifies 3 features"));
        }
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClassificationStreamProcessorExtension10() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier(0.2, " + "0.0, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClassificationStreamProcessorExtension11() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('m1', " + "0.0, " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClassificationStreamProcessorExtension12() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('m1', " + "0.0, 0.5, " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" + "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testClassificationStreamProcessorExtension13() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - default bias, threshold");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', attribute_0, " +
                "attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                + trainingQuery + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                // should emit probabilities
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, true, 0.5263760000000001},
                            inEvents[0].getData());
                } else if (count.get() == 2) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.33, 0.23, 0.632, 0.992, false, 0.2779144},
                            inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamTrain");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{1.0, 1.0, 0.2, 0.13, "true"});
            inputHandler.send(new Object[]{0.9, 0.89, 0.3, 0.02, "true"});
            inputHandler.send(new Object[]{0.0, 0.0, 1.0, 0.82, "false"});
            inputHandler.send(new Object[]{0.01, 0.4, 0.77, 0.92, "false"});
            inputHandler.send(new Object[]{0.80, 0.81, 0.11, 0.13, "true"});
            inputHandler.send(new Object[]{0.02, 0.30, 0.88, 0.76, "false"});
            inputHandler.send(new Object[]{0.93, 0.71, 0.02, 0.122, "true"});
            inputHandler.send(new Object[]{0.29, 0.24, 0.98, 0.65, "false"});
            Thread.sleep(5000);

            InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("StreamA");
            // send some unseen data for prediction
            inputHandler1.send(new Object[]{0.8, 0.67, 0.1, 0.03});
            inputHandler1.send(new Object[]{0.33, 0.23, 0.632, 0.992});

            SiddhiTestHelper.waitForEvents(200, 2, count, 60000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension14() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - init predict first and then "
                + "update model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', attribute_0, "
                + "attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream
                    + inStreamDefinition + query + trainingQuery);

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Model [model1.PerceptronTestApp] "
                    + "needs to initialized prior to be used with streamingml:perceptronClassifier. Perform "
                    + "streamingml:updatePerceptronClassifier process first"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension15() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - Incompatible model (reverse)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0, 0.5,"
                + "attribute_0, attribute_1, attribute_2) \n" + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                    + trainingQuery + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Model [model1] expects 4 features, but the "
                    + "streamingml:perceptronClassifier specifies 3 features"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension16() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - threshold is less than 0");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0," +
                "-0.1, attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into " +
                "outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter value found for "
                    + "the model.threshold argument. Expected a value between 0 & 1, but found: -0.1"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension17() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - model is visible only within the " +
                "SiddhiApp");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('PerceptronTestApp2') \ndefine stream StreamA (attribute_0 double, " +
                "attribute_1 double, attribute_2 " + "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0," +
                "0.6, attribute_0, attribute_1, attribute_2) \n" + "insert all events into " + "outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(trainingStream + trainingQuery);
            // should be successful even though both the apps are using the same model name with different feature
            // values
            SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.fail("Model is visible across Siddhi Apps which is wrong!");
        }
    }
}
