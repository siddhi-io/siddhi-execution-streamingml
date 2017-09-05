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
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Scanner;

/**
 * Testing @{@link PerceptronClassifierUpdaterStreamProcessorExtension}
 */
public class PerceptronClassifierUpdaterStreamProcessorExtensionTestCase {

    private static final Logger logger = Logger.getLogger(PerceptronClassifierUpdaterStreamProcessorExtensionTestCase
            .class);
    private volatile int count;

    @BeforeMethod
    public void init() {
        count = 0;
    }

    @Test
    public void testClassificationStreamProcessorExtension1() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Assert updated weights");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1', " +
                "attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n" + "insert all events into" +
                " outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, "true", 0.001, 0.008, 0.002,
                            3.0E-4}, inEvents[0].getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.1, 0.65, 0.92, "false", 0.003, 0.0175,
                            0.004200000000000001, 0.0013}, inEvents[0].getData());
                }
            }
        });
        Scanner scanner = null;
        try {
            File file = new File("src/test/resources/perceptron.csv");
            InputStream inputStream = new FileInputStream(file);
            Reader fileReader = new InputStreamReader(inputStream, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            scanner = new Scanner(bufferedReader);

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            while (scanner.hasNext()) {
                String eventStr = scanner.nextLine();
                String[] event = eventStr.split(",");
                inputHandler.send(new Object[]{Double.valueOf(event[0]), Double.valueOf(event[1]), Double.valueOf
                        (event[2]), Double.valueOf(event[3]), event[4]});

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            Thread.sleep(1100);
            AssertJUnit.assertEquals(8, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension2() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Label in the middle");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "string, attribute_3 double, attribute_4 double );";
        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:updatePerceptronClassifier('model1',attribute_2, " +
                "0.01, attribute_0, attribute_1, attribute_3, attribute_4) \n" +
                "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.1, "false", 0.65, 0.92, 0.003, 0.0175,
                            0.004200000000000001, 0.0013}, inEvents[0]
                            .getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, "true", 0.2, 0.03});
            inputHandler.send(new Object[]{0.2, 0.95, "true", 0.22, 0.1});
            inputHandler.send(new Object[]{0.8, 0.1, "false", 0.65, 0.92});
            inputHandler.send(new Object[]{0.75, 0.1, "false", 0.58, 0.71});
            Thread.sleep(1100);
            AssertJUnit.assertEquals(4, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension3() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Features are not of type double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1', " +
                "attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("model.features in 7th parameter is not a numerical"
                    + " type attribute. Found BOOL. Check the input stream definition"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension4() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Label is not of type string or " +
                "bool");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 int );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'," +
                "attribute_4,0.01, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("[model.label] attribute_4 in updatePerceptronClassifier " +
                    "should be either a BOOL or a STRING (true/false). But found INT"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension5() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Label string not true/false");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'," +
                "attribute_4,0.01, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, "truee"});
            Thread.sleep(2000);
            AssertJUnit.assertEquals(0, count);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension6() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Label is of bool type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 bool );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'," +
                "attribute_4,0.01, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.1, 0.65, 0.92, "false", 0.003, 0.0175,
                            0.004200000000000001, 0.0013}, inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, "true"});
            inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, "true"});
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, "false"});
            inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, "false"});
            Thread.sleep(1100);
            AssertJUnit.assertEquals(4, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension7() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Restore from a restart");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        String inStreamDefinition = "@App:name('PerceptronTestApp') \ndefine stream StreamA (attribute_0 double, " +
                "attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 bool );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'," +
                "attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, "true", 0.001, 0.008, 0.002,
                            3.0E-4}, inEvents[0].getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.2, 0.95, 0.22, 0.1, "true", 0.003, 0.0175,
                            0.004200000000000001, 0.0013}, inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, "true"});
            // persist
            siddhiManager.persist();
            Thread.sleep(5000);
            // send few more events to change the weights
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, "false"});
            inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, "true"});
            inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, "false"});
            Thread.sleep(1000);
            // shutdown the app
            siddhiAppRuntime.shutdown();

            // recreate the same app
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {

                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    count++;
                    if (count == 5) {
                        // weights should be restored and should be following
                        AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.1, 0.65, 0.92, "false", 0.001, 0.008, 0.002,
                                3.0E-4}, inEvents[0]
                                .getData());
                    }
                }
            });
            // start the app
            siddhiAppRuntime.start();
            // restore
            siddhiManager.restoreLastState();
            inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            // send a new event
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, "false"});

            Thread.sleep(1100);
            AssertJUnit.assertEquals(5, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension8() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - learning rate is not double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'," +
                "attribute_4, 1, attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found for the learning.rate " +
                    "argument. Expected: DOUBLE but found: INT"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension9() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier(attribute_4," +
                "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

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
    public void testClassificationStreamProcessorExtension10() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier() \n" +
                "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                    query);
            AssertJUnit.fail();
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            logger.error(e);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of parameters [0] for " +
                    "streamingml:updatePerceptronClassifier"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension11() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - default learning rate");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'," +
                "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.2, 0.95, 0.22, 0.1, "true", 0.030000000000000006,
                            0.17500000000000002, 0.04200000000000001, 0.013000000000000001}, inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, "true"});
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, "false"});
            inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, "true"});
            inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, "false"});
            Thread.sleep(1100);
            AssertJUnit.assertEquals(4, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension12() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier(0.2,attribute_4, " +
                "attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found for the model.name argument," +
                    " required STRING but found DOUBLE"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension13() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('m1',attribute_4," +
                "1.0, attribute_0, attribute_1, attribute_2, 2)" +
                "\ninsert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("7th parameter is not an attribute "
                    + "(VariableExpressionExecutor) present in the stream definition. Found a "
                    + "org.wso2.siddhi.core.executor.ConstantExpressionExecutor"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension14() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('m1',attribute_4," +
                "1.0, attribute_0, attribute_1, attribute_2, attribute_3, 2)" +
                "\ninsert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid number of parameters for " +
                    "streamingml:updatePerceptronClassifier. This Stream Processor requires at most 7 parameters, " +
                    "namely, model.name, model.label, learning.rate, model.features but found 8 parameters"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension15() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - model.label is not an attribute");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('m1',2," +
                "1.0, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("model.label attribute in updatePerceptronClassifier " +
                    "should be a variable, but found a org.wso2.siddhi.core.executor.ConstantExpressionExecutor"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension16() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - label as bool");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 bool );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'" +
                ", attribute_4, attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.2, 0.95, 0.22, 0.1, true, 0.030000000000000006,
                            0.17500000000000002, 0.04200000000000001, 0.013000000000000001}, inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, true});
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, false});
            inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, true});
            inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, false});
            Thread.sleep(1100);
            AssertJUnit.assertEquals(4, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension17() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - lesser features");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 bool );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'" +
                ", attribute_4, attribute_0) \n" +
                "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.2, 0.95, 0.22, 0.1, true, 0.030000000000000006},
                            inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, true});
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, false});
            inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, true});
            inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, false});
            Thread.sleep(1100);
            AssertJUnit.assertEquals(4, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension18() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Restore from a restart");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        String inStreamDefinition = "@App:name('PerceptronTestApp') \ndefine stream StreamA (attribute_0 double, " +
                "attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 bool );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1'," +
                "attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, "true", 0.001, 0.008, 0.002,
                            3.0E-4}, inEvents[0].getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.2, 0.95, 0.22, 0.1, "true", 0.003, 0.0175,
                            0.004200000000000001, 0.0013}, inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, "true"});
            // persist
            siddhiManager.persist();
            Thread.sleep(5000);
            // send few more events to change the weights
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, "false"});
            inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, "true"});
            inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, "false"});
            Thread.sleep(1000);
            // shutdown the app
            siddhiAppRuntime.shutdown();

            // recreate the same app
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {

                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    count++;
                    if (count == 5) {
                        // as the model is new, we should see the same result as count==1
                        AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, "true", 0.001, 0.008, 0.002,
                                3.0E-4}, inEvents[0].getData());
                    }

                }
            });
            // start the app
            siddhiAppRuntime.start();
            inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            // send a new event
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, "true"});

            Thread.sleep(1100);
            AssertJUnit.assertEquals(5, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}
