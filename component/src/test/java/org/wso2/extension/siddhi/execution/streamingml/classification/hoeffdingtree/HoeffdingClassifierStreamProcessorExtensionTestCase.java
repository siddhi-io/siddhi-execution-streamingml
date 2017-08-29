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
package org.wso2.extension.siddhi.execution.streamingml.classification.hoeffdingtree;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

public class HoeffdingClassifierStreamProcessorExtensionTestCase {
    private static final Logger logger = Logger
            .getLogger(HoeffdingClassifierStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private String trainingStream = "@App:name('HoeffdingTestApp') \n" +
            "define stream StreamTrain (attribute_0 double, " +
            "attribute_1 double, attribute_2 double, attribute_3 double, attribute_4 string );";
    private String trainingQuery = ("@info(name = 'query-train') " +
            "from StreamTrain#streamingml:hoeffdingLearn('model1', 4, " +
            "attribute_0, attribute_1, attribute_2, attribute_3, attribute_4) \n"
            + "insert all events into trainOutputStream;\n");

    @BeforeMethod
    public void init() {
        count = 0;
    }

    @Test
    public void testClassificationStreamProcessorExtension1() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Assert predictions and evolution");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, " +
                "attribute_1 double, attribute_2 double, attribute_3 double);";
        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:hoeffdingPredict('model1', " +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                + trainingQuery + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{7.2, 3, 5.8, 1.6, "versicolor", 1.0},
                            inEvents[0].getData());
                } else if (count == 2) {
                    AssertJUnit.assertArrayEquals(new Object[]{5.8, 2.7, 5.1, 1.9, "versicolor", 1.0},
                            inEvents[0].getData());
                } else if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{4.9, 3.1, 1.5, 0.1, "setosa", 1.0},
                            inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamTrain");
            siddhiAppRuntime.start();

            inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});
            inputHandler.send(new Object[]{4.9, 3, 1.4, 0.2, "setosa"});
            inputHandler.send(new Object[]{5.5, 2.5, 4, 1.3, "versicolor"});
            inputHandler.send(new Object[]{5.4, 3.9, 1.3, 0.4, "setosa"});
            inputHandler.send(new Object[]{6.8, 2.8, 4.8, 1.4, "versicolor"});
            inputHandler.send(new Object[]{6.4, 3.1, 5.5, 1.8, "virginica"});
            inputHandler.send(new Object[]{6.8, 3, 5.5, 2.1, "virginica"});
            inputHandler.send(new Object[]{4.8, 3.4, 1.9, 0.2, "setosa"});

            Thread.sleep(1100);

            InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("StreamA");
            // send some unseen data for prediction
            inputHandler1.send(new Object[]{7.2, 3, 5.8, 1.6});
            inputHandler1.send(new Object[]{5.8, 2.7, 5.1, 1.9});
            inputHandler1.send(new Object[]{4.9, 3.1, 1.5, 0.1});

            Thread.sleep(1000);
            AssertJUnit.assertEquals(3, count);
            Thread.sleep(1100);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension2() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - Features are not of numeric type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict('model1', " +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("model.features in 5th parameter is not " +
                    "a numerical type attribute. Found BOOL. Check the input stream definition"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension3() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - model.name is not String");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict(123, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found for " +
                    "the model.name argument, required STRING but found INT"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension4() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict(attribute_4, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

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
    public void testClassificationStreamProcessorExtension5() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict() \n" + "insert all " +
                "events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            logger.error(e);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of parameters for ml:hoeffdingPredict. " +
                    "This Stream Processor requires at least 3 parameters"));
        }
    }


    @Test
    public void testClassificationStreamProcessorExtension6() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - Incompatible model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict('model1', " +
                "attribute_0, attribute_1, attribute_2) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream +
                    inStreamDefinition + trainingQuery + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            logger.error(e);
            AssertJUnit.assertTrue(e.getMessage().contains("Model [model1] expects 5 features, " +
                    "but the streamingml:hoeffdingClassifier specifies 3 features"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension7() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict(0.2, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found " +
                    "for the model.name argument, required STRING but found DOUBLE"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension8() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict('m1', " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of parameters for " +
                    "streamingml:hoeffdingClassifier. This Stream Processor requires at most 5 parameters, namely, " +
                    "model.name, model.features but found 6 parameters"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension9() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict('m1', " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of parameters for " +
                    "streamingml:hoeffdingClassifier. This Stream Processor requires at most 5 parameters, namely, " +
                    "model.name, model.features but found 6 parameters"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension10() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Incompatible model (reverse)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict('model1', " +
                "attribute_0, attribute_1, attribute_2) \n" + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream +
                    inStreamDefinition + query + trainingQuery);
            AssertJUnit.fail();
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            logger.error(e);
            AssertJUnit.assertTrue(e.getMessage().contains(" Model [model1] expects 5 features, " +
                    "but the streamingml:hoeffdingClassifier specifies 3 features"));
        }
    }


    /*@Test
    public void testClassificationStreamProcessorExtension14() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - init predict first and then " +
                "update model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingPredict('model1', attribute_0, " +
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
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Model [model1.HoeffdingTestApp] expects 3 features, " +
                    "but the streamingml:hoeffdingClassifier specifies 5 features"));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }*/


}
