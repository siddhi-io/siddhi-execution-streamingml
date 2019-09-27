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

package io.siddhi.extension.execution.streamingml.bayesian.regression;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.extension.execution.streamingml.UnitTestAppender;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class BayesianRegressionUpdaterStreamProcessorExtensionTestCase {
    private static final Logger logger = Logger.getLogger(
            BayesianRegressionUpdaterStreamProcessorExtensionTestCase.class);
    private AtomicInteger count;

    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension1() throws InterruptedException {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - All params in");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1', "
                + "attribute_4, 1, 'adam', 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{0.1, 0.8, 0.3, 0.2, 1.0});
        inputHandler.send(new Object[]{0.2, 0.95, 0.42, 0.22, 2.8});
        inputHandler.send(new Object[]{0.8, 0.10, 0.8, 0.65, 3.1});
        inputHandler.send(new Object[]{0.75, 0.1, 0.43, 0.58, 1.1});
        SiddhiTestHelper.waitForEvents(200, 4, count, 1000);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension2() throws InterruptedException {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Only learning rate in");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1', "
                + "attribute_4, 'nadam', 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{0.1, 0.8, 0.1, 0.2, 0.03});
        inputHandler.send(new Object[]{0.2, 0.95, 0.42, 0.22, 0.1});
        inputHandler.send(new Object[]{0.8, 0.1, 0.8, 0.65, 0.92});
        inputHandler.send(new Object[]{0.75, 0.1, 0.4, 0.58, 0.71});
        SiddhiTestHelper.waitForEvents(200, 4, count, 5000);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension3() throws InterruptedException {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - all parameters default");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1', "
                + "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{0.1, 0.8, 0.1, 0.2, 0.53});
        inputHandler.send(new Object[]{0.2, 0.95, 0.42, 0.22, 0.1});
        inputHandler.send(new Object[]{0.8, 0.1, 0.8, 0.65, 0.92});
        inputHandler.send(new Object[]{0.75, 0.1, 0.4, 0.58, 0.21});
        SiddhiTestHelper.waitForEvents(200, 4, count, 5000);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension4() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Not implemented optimizer given");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1'," +
                "attribute_4, 'adaam', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        UnitTestAppender testAppender = new UnitTestAppender();
        Logger logger = Logger.getLogger(SiddhiAppRuntime.class);
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            logger.addAppender(testAppender);
        } catch (Exception e) {
            if (testAppender.getMessages() != null) {
                AssertJUnit.assertTrue(testAppender.getMessages().contains("model.optimizer should be one of " +
                        "[ADAM, ADAGRAD, SGD, NADAM]. But found adaam"));
            }
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension5() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Wrong parameter order (1)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1'," +
                "attribute_4, 0.01, 1, attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        UnitTestAppender testAppender = new UnitTestAppender();
        Logger logger = Logger.getLogger(SiddhiAppRuntime.class);
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            logger.addAppender(testAppender);
        } catch (Exception e) {
            if (testAppender.getMessages() != null) {
                AssertJUnit.assertTrue(e.getCause().getMessage().contains("Parameter 3 must either be a constant " +
                        "(hyperparameter) or an attribute of the stream (model.features), " +
                        "but found a io.siddhi.core.executor.ConstantExpressionExecutor"));
            }
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension6() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Wrong parameter order (2)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1'," +
                "attribute_4, 'adam', 10, attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        UnitTestAppender testAppender = new UnitTestAppender();
        Logger logger = Logger.getLogger(SiddhiAppRuntime.class);
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            logger.addAppender(testAppender);
        } catch (Exception e) {
            if (testAppender.getMessages() != null) {
                AssertJUnit.assertTrue(testAppender.getMessages().contains("3th parameter cannot be type of INT. " +
                        "Only model.sample can be INT, which can be set as the 2th parameter"));
            }
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianRegressionStreamProcessorExtension7() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - duplicated parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1'," +
                "attribute_4, 'adam', 'sgd', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension8() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Negative learning rate given");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1'," +
                "attribute_4, -0.01, attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        UnitTestAppender testAppender = new UnitTestAppender();
        Logger logger = Logger.getLogger(SiddhiAppRuntime.class);
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            logger.addAppender(testAppender);
        } catch (Exception e) {
            if (testAppender.getMessages() != null) {
                AssertJUnit.assertTrue(testAppender.getMessages().contains("learning.rate should be greater than " +
                        "zero. But found -0.010000"));
            }
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianRegressionStreamProcessorExtension9() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Features are not of type double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 bool, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1', "
                + "attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianRegressionStreamProcessorExtension10() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Target is not of type double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1'," +
                "attribute_4,0.01, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension11() throws InterruptedException {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Features, target type are integer");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 int, attribute_2 " +
                "double, attribute_3 double, attribute_4 int );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1'," +
                "attribute_4,0.01, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{0.10, 0.87, 0.1, 0.2, 1});
        inputHandler.send(new Object[]{0.2, 5, 0.42, 0.22, 2});
        inputHandler.send(new Object[]{0.8, 1, 0.8, 0.65, 3});
        inputHandler.send(new Object[]{0.75, 3, 0.43, 0.58, 1});
        SiddhiTestHelper.waitForEvents(200, 4, count, 1000);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianRegressionStreamProcessorExtension12() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression(attribute_4,"
                + "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3)"
                + "\ninsert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianRegressionStreamProcessorExtension13() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression() \n" +
                "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testBayesianRegressionStreamProcessorExtension14() {

        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression(0.2,attribute_4, " +
                "attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension15() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - duplicated model names");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 int, attribute_4 double );";
        String query1 = ("from StreamA#streamingml:updateBayesianRegression('model1',"
                + "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3)"
                + "\ninsert all events into outputStream;");
        String query2 = ("from StreamA#streamingml:updateBayesianRegression('model1',"
                + "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3)"
                + "\ninsert all events into outputStream;");
        UnitTestAppender testAppender = new UnitTestAppender();
        Logger logger = Logger.getLogger(SiddhiAppRuntime.class);
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition
                    + query1 + query2);
            logger.addAppender(testAppender);
        } catch (Exception e) {
            if (testAppender.getMessages() != null) {
                AssertJUnit.assertTrue(testAppender.getMessages().contains("A model already exists with name the " +
                        "model1. Use a different value for model.name argument."));
            }
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension16() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('m1',attribute_4,"
                + "1.0, attribute_0, attribute_1, attribute_2, 2)\n"
                + "insert all events into outputStream;");
        UnitTestAppender testAppender = new UnitTestAppender();
        Logger logger = Logger.getLogger(SiddhiAppRuntime.class);
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            logger.addAppender(testAppender);
        } catch (Exception e) {
            if (testAppender.getMessages() != null) {
                AssertJUnit.assertTrue(testAppender.getMessages().contains("7th parameter is not an attribute "
                        + "(VariableExpressionExecutor) present in the stream definition. Found a "
                        + "io.siddhi.core.executor.ConstantExpressionExecutor"));
            }
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension17() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('m1',attribute_4," +
                "10, 'adam', 1.0, attribute_0, attribute_1, attribute_2, attribute_3, 2)" +
                "\ninsert all events into outputStream;");
        UnitTestAppender testAppender = new UnitTestAppender();
        Logger logger = Logger.getLogger(SiddhiAppRuntime.class);
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            logger.addAppender(testAppender);
        } catch (Exception e) {
            if (testAppender.getMessages() != null) {
                AssertJUnit.assertTrue(testAppender.getMessages().contains("Invalid number of parameters for " +
                        "streamingml:updateBayesianRegression. This Stream Processor requires at most 9 parameters, " +
                        "namely, model.name, model.target, model.samples[optional], model.optimizer[optional], " +
                        "learning.rate[optional], model.features. but found 10 parameters"));
            }
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension18() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - model.target is not an attribute");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('m1',2," +
                "1.0, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");
        UnitTestAppender testAppender = new UnitTestAppender();
        Logger logger = Logger.getLogger(SiddhiAppRuntime.class);
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            logger.addAppender(testAppender);
        } catch (Exception e) {
            if (testAppender.getMessages() != null) {
                AssertJUnit.assertTrue(testAppender.getMessages().contains("model.target attribute in "
                        + "updateBayesianRegression should be a variable, but found a "
                        + "io.siddhi.core.executor.ConstantExpressionExecutor"));
            }
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension19() {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - model is visible only within the " +
                "SiddhiApp");
        SiddhiManager siddhiManager = new SiddhiManager();

        String trainingStream = "@App:name('BayesianRegressionTestApp1') \n"
                + "define stream StreamTrain (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 double );";
        String trainingQuery = ("@info(name = 'query-train') from " +
                "StreamTrain#streamingml:updateBayesianRegression" + "('model1', attribute_4, 0.1, attribute_0, " +
                "attribute_1, attribute_2, attribute_3) \n" + "insert all events into trainOutputStream;\n");

        String inStreamDefinition = "@App:name('BayesianRegressionTestApp2') \n"
                + "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double );";
        String query = ("@info(name = 'query1') from "
                + "StreamA#streamingml:updateBayesianRegression('model1', attribute_3, 0.1, attribute_0, " +
                "attribute_1, attribute_2) \n" + "insert all events into " + "outputStream;");

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(trainingStream + trainingQuery);
        // should be successful even though both the apps are using the same model name with different feature
        // values
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiManager.shutdown();
    }

    @Test
    public void testBayesianRegressionStreamProcessorExtension20() throws InterruptedException {
        logger.info("BayesianRegressionUpdaterStreamProcessorExtension TestCase - Restore from a restart");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 double);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianRegression('model1', "
                + "attribute_4, 1, 'adam', 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{0.1, 0.8, 0.3, 0.2, 1.0});

        // persist
        siddhiManager.persist();
        Thread.sleep(5000);

        // send few more events to change the weights
        inputHandler.send(new Object[]{0.2, 0.95, 0.42, 0.22, 2.8});
        inputHandler.send(new Object[]{0.8, 0.10, 0.8, 0.65, 3.1});
        inputHandler.send(new Object[]{0.75, 0.1, 0.43, 0.58, 1.1});
        Thread.sleep(1000);

        // shutdown the app
        siddhiAppRuntime.shutdown();

        // recreate the same app
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
            }
        });

        // start the app
        siddhiAppRuntime.start();
        // restore
        siddhiManager.restoreLastState();
        inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
        // send a new event
        inputHandler.send(new Object[]{0.35, 0.55, 0.12, 0.65, 0.9});
        SiddhiTestHelper.waitForEvents(200, 5, count, 5000);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
