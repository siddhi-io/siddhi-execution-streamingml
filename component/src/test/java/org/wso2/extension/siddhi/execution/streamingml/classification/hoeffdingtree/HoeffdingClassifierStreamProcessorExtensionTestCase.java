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
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class HoeffdingClassifierStreamProcessorExtensionTestCase {
    private static final Logger logger = Logger
            .getLogger(HoeffdingClassifierStreamProcessorExtensionTestCase.class);
    private AtomicInteger count;
    private String trainingStream = "@App:name('HoeffdingTestApp') \n" +
            "define stream StreamTrain (attribute_0 double, " +
            "attribute_1 double, attribute_2 double, attribute_3 double, attribute_4 string );";
    private String trainingQuery = ("@info(name = 'query-train') " +
            "from StreamTrain#streamingml:updateHoeffdingTree('ml', 4, " +
            "attribute_0, attribute_1, attribute_2, attribute_3, attribute_4) \n"
            + "insert all events into trainOutputStream;\n");

    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }


    @Test
    public void testClassificationStreamProcessorExtension1() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Assert predictions and evolution");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, " +
                "attribute_2 double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier('ml', " +
                " attribute_0, attribute_1, attribute_2, attribute_3) " +
                "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                + trainingQuery + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{5.1, 3.8, 1.6, 0.2, "setosa", 1.0},
                            inEvents[0].getData());
                } else if (count.get() == 2) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.5, 2.8, 4.6, 1.5, "versicolor", 1.0},
                            inEvents[0].getData());
                } else if (count.get() == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{5.7, 2.5, 5, 2, "versicolor", 1.0},
                            inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamTrain");
            siddhiAppRuntime.start();

            inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
            inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
            inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
            inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
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
            inputHandler1.send(new Object[]{5.1, 3.8, 1.6, 0.2});
            inputHandler1.send(new Object[]{6.5, 2.8, 4.6, 1.5});
            inputHandler1.send(new Object[]{5.7, 2.5, 5, 2});

            SiddhiTestHelper.waitForEvents(200, 3, count, 60000);
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
                "double, attribute_3 bool );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier('model1', " +
                " attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getMessage());
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
                "double, attribute_3 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier(123, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" + "" +
                "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getMessage());
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
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier(attribute_4, " +
                "attribute_0, attribute_1, attribute_2, attribute_3, attribute_4) \n"
                + "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel" +
                " insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getMessage());
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
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier() \n"
                + "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of parameters " +
                    "for streamingml:hoeffdingTreeClassifier. This Stream Processor requires at least 3 parameters," +
                    " namely, model.name and at least 2 feature_attributes, but found 0 parameters"));
        }
    }


    @Test
    public void testClassificationStreamProcessorExtension6() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - Incompatible model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier('model1', " +
                "attribute_0, attribute_1, attribute_2) \n"
                + "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream +
                    inStreamDefinition + trainingQuery + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of feature attributes " +
                    "for streamingml:hoeffdingTreeClassifier. This Stream Processor is defined with " +
                    "4 features, but found 3 feature"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension7() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier(0.2, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
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
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier('m1', " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n"
                + "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of feature attributes " +
                    "for streamingml:hoeffdingTreeClassifier. This Stream Processor is defined with 4 features," +
                    " but found 5 feature attributes"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension9() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier('m1', " +
                "attribute_0, attribute_1, attribute_2, attribute_3, 2) \n" +
                "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of feature attributes " +
                    "for streamingml:hoeffdingTreeClassifier. This Stream Processor is defined with 4 features," +
                    " but found 5 feature attributes"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension10() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Incompatible model (reverse)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier('model1', " +
                "attribute_0, attribute_1, attribute_2) \n"
                + "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream +
                    inStreamDefinition + query + trainingQuery);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of feature attributes "
                    + "for streamingml:hoeffdingTreeClassifier. This Stream Processor is defined with 4 features, "
                    + "but found 3 feature attributes"));
        }
    }


    @Test
    public void testClassificationStreamProcessorExtension11() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - init predict before " +
                "training the model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";
        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:hoeffdingTreeClassifier('model1', attribute_0, " +
                "attribute_1, attribute_2, attribute_3) \n"
                + "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                    + query + trainingQuery);

        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Model [HoeffdingTestApp.model1] needs to "
                    + "initialized prior to be used with streamingml:hoeffdingTreeClassifier. "
                    + "Perform streamingml:updateHoeffdingTree process first."));
        }
    }


    @Test
    public void testClassificationStreamProcessorExtension12() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Input feature value attributes mismatch from the feature attribute definition");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, " +
                "attribute_2 double, attribute_3 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingTreeClassifier('ml', " +
                " attribute_0, attribute_1, attribute_2, attribute_3) " +
                "select attribute_0, attribute_1, attribute_2, attribute_3, prediction, confidenceLevel " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(trainingStream + inStreamDefinition
                + trainingQuery + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);

            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamTrain");
            siddhiAppRuntime.start();

            inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
            inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});

            Thread.sleep(1100);

            InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("StreamA");
            // send some unseen data for prediction
            inputHandler1.send(new Object[]{5.1, "setosa", 1.6, 0.2});

        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppRuntimeException);
            AssertJUnit.assertTrue(e.getMessage().contains("Incompatible attribute feature type at position 2. "
                    + "Not of any numeric type. Please refer the stream definition for Model[HoeffdingTestApp.ml]"));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}


