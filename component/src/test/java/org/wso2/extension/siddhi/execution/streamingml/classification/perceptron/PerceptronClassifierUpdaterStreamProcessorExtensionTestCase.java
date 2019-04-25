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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testing @{@link PerceptronClassifierUpdaterStreamProcessorExtension}
 */
public class PerceptronClassifierUpdaterStreamProcessorExtensionTestCase {

    private static final Logger logger = Logger.getLogger(PerceptronClassifierUpdaterStreamProcessorExtensionTestCase
            .class);
    private AtomicInteger count;

    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }

    @Test
    public void testClassificationStreamProcessorExtension1() throws InterruptedException, FileNotFoundException,
            UnsupportedEncodingException {
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
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, "true", 0.001, 0.008, 0.002,
                            3.0E-4}, inEvents[0].getData());
                }
                if (count.get() == 3) {
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
                    logger.error(e.getCause().getMessage());
                }
            }
            SiddhiTestHelper.waitForEvents(200, 8, count, 60000);
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
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 3) {
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

            SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension3() {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Features are not of type double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 bool, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1', "
                + "attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("model.features in 7th parameter is not"
                    + " a numerical type attribute. Found BOOL. Check the input stream definition"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension4() {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Label is not of type string or "
                + "bool");
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
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("[model.label] attribute_4 in "
                    + "updatePerceptronClassifier should be either a BOOL or a STRING (true/false). But found INT"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension5() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Label string not true/false");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('model1',"
                + "attribute_4,0.01, attribute_0, attribute_1, attribute_2, attribute_3)\n"
                + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, "truee"});
            SiddhiTestHelper.waitForEvents(200, 0, count, 60000);
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

                count.incrementAndGet();
                if (count.get() == 3) {
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

            SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
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
                count.incrementAndGet();
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, "true", 0.001, 0.008, 0.002,
                            3.0E-4}, inEvents[0].getData());
                }
                if (count.get() == 3) {
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

                    count.incrementAndGet();
                    if (count.get() == 5) {
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
            SiddhiTestHelper.waitForEvents(200, 5, count, 60000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension8() {
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
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter type found for the "
                    + "learning.rate argument. Expected: DOUBLE but found: INT"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension9() {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier(attribute_4,"
                + "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3)"
                + "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Parameter model.name must be a "
                    + "constant but found io.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension10() {
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
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid number of parameters [0] for "
                    + "streamingml:updatePerceptronClassifier"));
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
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 3) {
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

            SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension12() {

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
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter type found for the "
                    + "model.name argument, required STRING but found DOUBLE"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension13() {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updatePerceptronClassifier('m1',attribute_4,"
                + "1.0, attribute_0, attribute_1, attribute_2, 2)\n"
                + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("7th parameter is not an attribute "
                    + "(VariableExpressionExecutor) present in the stream definition. Found a "
                    + "io.siddhi.core.executor.ConstantExpressionExecutor"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension14() {
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
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid number of parameters for " +
                    "streamingml:updatePerceptronClassifier. This Stream Processor requires at most 7 parameters, " +
                    "namely, model.name, model.label, learning.rate, model.features but found 8 parameters"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension15() {
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
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("model.label attribute in "
                    + "updatePerceptronClassifier should be a variable, but found a "
                    + "io.siddhi.core.executor.ConstantExpressionExecutor"));
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
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 3) {
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

            SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
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
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 3) {
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

            SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
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
                count.incrementAndGet();
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, "true", 0.001, 0.008, 0.002,
                            3.0E-4}, inEvents[0].getData());
                }
                if (count.get() == 3) {
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
                    count.incrementAndGet();
                    if (count.get() == 5) {
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

            SiddhiTestHelper.waitForEvents(200, 5, count, 60000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension19() {
        logger.info("PerceptronClassifierStreamProcessorExtension TestCase - model is visible only within the " +
                "SiddhiApp");
        SiddhiManager siddhiManager = new SiddhiManager();

        String trainingStream = "@App:name('PerceptronTestApp1') \n"
                + "define stream StreamTrain (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 string );";
        String trainingQuery = ("@info(name = 'query-train') from " +
                "StreamTrain#streamingml:updatePerceptronClassifier" + "('model1', attribute_4, 0.1, attribute_0, " +
                "attribute_1, attribute_2, attribute_3) \n" + "insert all events into trainOutputStream;\n");

        String inStreamDefinition = "@App:name('PerceptronTestApp2') \n"
                + "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 string );";
        String query = ("@info(name = 'query1') from "
                + "StreamA#streamingml:updatePerceptronClassifier('model1', attribute_3, 0.1, attribute_0, " +
                "attribute_1, attribute_2) \n" + "insert all events into " + "outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(trainingStream + trainingQuery);
            // should be successful even though both the apps are using the same model name with different feature
            // values
            SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.fail("Model is visible across Siddhi Apps which is wrong!");
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension20() throws InterruptedException {
        logger.info("PerceptronClassifierUpdaterStreamProcessorExtension TestCase - Features are both int and double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "int, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:updatePerceptronClassifier('model1',attribute_4, " +
                "0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.8, 0.1, 1, 0.92, "false", 0.003, 0.0175,
                            0.03, 0.0013}, inEvents[0]
                            .getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 1, 0.03, "true"});
            inputHandler.send(new Object[]{0.2, 0.95, 2, 0.1, "true"});
            inputHandler.send(new Object[]{0.8, 0.1, 1, 0.92, "false"});
            inputHandler.send(new Object[]{0.75, 0.1, 3, 0.71, "false"});

            SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

}
