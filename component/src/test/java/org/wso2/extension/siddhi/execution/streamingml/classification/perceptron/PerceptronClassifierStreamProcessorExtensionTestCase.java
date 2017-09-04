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

package org.wso2.extension.siddhi.execution.streamingml.classification.perceptron;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * Testing @{@link PerceptronClassifierStreamProcessorExtension}
 */
public class PerceptronClassifierStreamProcessorExtensionTestCase {

    private static final Logger logger = Logger.getLogger(PerceptronClassifierStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private String trainingStream = "@App:name('PerceptronTestApp') \ndefine stream StreamTrain (attribute_0 double, " +
            "attribute_1 double, attribute_2 " + "double, attribute_3 double, attribute_4 string );";
    private String trainingQuery = ("@info(name = 'query-train') from " +
            "StreamTrain#streamingml:updatePerceptronClassifier" + "('model1', attribute_4, 0.1, attribute_0, " +
            "attribute_1, attribute_2, attribute_3) \n" + "insert all events into trainOutputStream;\n");


    @BeforeMethod
    public void init() {
        count = 0;
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
                count++;
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, true, 0.5263760000000001},
                            inEvents[0].getData());
                } else if (count == 2) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.33, 0.23, 0.632, 0.992, false, 0.2779144},
                            inEvents[0].getData());
                } else if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, false, 0.333926}, inEvents[0]
                            .getData());
                } else if (count == 4) {
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

            AssertJUnit.assertEquals(4, count);
            Thread.sleep(1100);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension2() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - Features are not of type double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0,0.5," +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("model.features in perceptronClassifier should be of" + " " +
                    "type DOUBLE or INT. But there's an attribute called attribute_3 of type BOOL"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension3() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - bias is not double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "2,0.5, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found for the model.bias argument" +
                    "." + " Expected: DOUBLE but found: INT"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension4() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - threshold is not double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0,2, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found for the model.threshold " +
                    "argument. Expected: DOUBLE but found: INT"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension5() throws InterruptedException {
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
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter value found for the model.threshold " +
                    "argument. Expected a value between 0 & 1, but found: 1.1"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension6() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier(attribute_4, " + "0.0," +
                "1.1, attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Parameter model.name must be a constant but found org" +
                    ".wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension7() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier() \n" + "insert all " +
                "events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            logger.error(e);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of parameters [0] for " +
                    "streamingml:perceptronClassifier"));
        }
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
                count++;
                EventPrinter.print(inEvents);
                // should emit probabilities
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, true, 0.5263760000000001},
                            inEvents[0].getData());
                } else if (count == 2) {
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
            AssertJUnit.assertEquals(2, count);
            Thread.sleep(1100);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension9() throws InterruptedException {
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
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            logger.error(e);
            AssertJUnit.assertTrue(e.getMessage().contains("Model [model1] expects 4 features, but the " +
                    "streamingml:perceptronClassifier specifies 3 features"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension10() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier(0.2, " + "0.0, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found for the model.name argument," +
                    "" + " required STRING but found DOUBLE"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension11() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('m1', " + "0.0, " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Parameter[7] of perceptronClassifier must be an " +
                    "attribute present in the stream, but found a org.wso2.siddhi.core.executor" + "" +
                    ".ConstantExpressionExecutor"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension12() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('m1', " + "0.0, 0.5, " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of parameters for " +
                    "streamingml:perceptronClassifier. This Stream Processor requires at most 7 parameters, " +
                    "namely, model.name, model.bias, model.threshold, model.features but found 8 parameters"));
        }
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
                count++;
                EventPrinter.print(inEvents);
                // should emit probabilities
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, true, 0.5263760000000001},
                            inEvents[0].getData());
                } else if (count == 2) {
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
            AssertJUnit.assertEquals(2, count);
            Thread.sleep(1100);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension14() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - init predict first and then " +
                "update model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', attribute_0, " +
                "attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                + query + trainingQuery);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                // should emit probabilities
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.67, 0.1, 0.03, true, 0.5263760000000001},
                            inEvents[0].getData());
                } else if (count == 2) {
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
            AssertJUnit.assertEquals(2, count);
            Thread.sleep(1100);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension15() throws InterruptedException {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - Incompatible model (reverse)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:perceptronClassifier('model1', " + "0.0, " +
                "attribute_0, attribute_1, attribute_2) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream +
                    inStreamDefinition + query + trainingQuery);
            AssertJUnit.fail();
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            logger.error(e);
            AssertJUnit.assertTrue(e.getMessage().contains("Model [model1] expects 3 features, but the " +
                    "streamingml:updatePerceptronClassifier specifies 4 features"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension16() throws InterruptedException {
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
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter value found for the model.threshold " +
                    "argument. Expected a value between 0 & 1, but found: -0.1"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension17() throws InterruptedException {
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
            logger.error(e.getMessage());
            AssertJUnit.fail("Model is visible across Siddhi Apps which is wrong!");
        }
    }

}
