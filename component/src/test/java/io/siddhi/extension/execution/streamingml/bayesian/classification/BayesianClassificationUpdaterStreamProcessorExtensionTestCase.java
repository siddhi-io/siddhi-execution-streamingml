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
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;


public class BayesianClassificationUpdaterStreamProcessorExtensionTestCase {
    private static final Logger logger = Logger.getLogger(
            BayesianClassificationUpdaterStreamProcessorExtensionTestCase.class);

    private AtomicInteger count;


    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension1() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Assert Model Build with " +
                "default parameters");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 string);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3, "
                + "attribute_4, 'nadam', 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

            try {
                InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
                siddhiAppRuntime.start();
                for (int i = 0; i < 10; i++) {
                    inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
                    inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
                    inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
                    inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
                    inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});
                    inputHandler.send(new Object[]{4.8, 3.4, 1.9, 0.2, "setosa"});
                    inputHandler.send(new Object[]{5.8, 2.7, 4.1, 1, "versicolor"});
                }

                SiddhiTestHelper.waitForEvents(200, 70, count, 1000);

            } catch (Exception e) {
                logger.error(e.getCause().getMessage());
                AssertJUnit.fail();
            } finally {
                siddhiAppRuntime.shutdown();
            }

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.fail("Model fails initialize with all the params");
        } finally {
            siddhiManager.shutdown();
        }

    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension2() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Assert model build "
                + "with manual configurations");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 string);";

        String query = ("@info(name = 'query1') "
                + "from StreamA#streamingml:updateBayesianClassification('model1', 3, attribute_4, 2, 'adam', 0.01, "
                + "attribute_0, attribute_1 , attribute_2 ,attribute_3)" +
                "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

            try {
                InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
                siddhiAppRuntime.start();
                inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
                inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
                inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
                inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
                inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});

                SiddhiTestHelper.waitForEvents(200, 5, count, 1000);
            } catch (Exception e) {
                logger.error(e.getCause().getMessage());
                AssertJUnit.fail();
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
    public void testBayesianClassificationStreamProcessorExtension3() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Duplicated model names "
                + "with manual configurations");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 string);";

        String query1 = ("from StreamA#streamingml:updateBayesianClassification('model1', 3, attribute_4, 2, " +
                "'adam', 0.01, attribute_0, attribute_1 , attribute_2 ,attribute_3)" +
                "insert all events into outputStream;");

        String query2 = ("from StreamA#streamingml:updateBayesianClassification('model1', 3, attribute_4, "
                + "attribute_0, attribute_1 , attribute_2 ,attribute_3)" +
                "insert all events into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query1
                    + query2);

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("A model already exists with name the model1. " +
                    "Use a different value for model.name argument."));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension4() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase -" +
                " Features are of un-supported type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 string, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3, "
                + "attribute_4, attribute_0 , attribute_1 ,attribute_2, attribute_3) "
                + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("model.features in 7th parameter is not a " +
                    "numerical type attribute. Found STRING. Check the input stream definition."));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension5() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - number of classes is missing");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1'," +
                "attribute_4, 'adam', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Parameter no.of.classes must be a constant. " +
                    "But found io.siddhi.core.executor.VariableExpressionExecutor"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension6() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - number of classes is not int");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3.1, " +
                "attribute_4, 'adam', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter type found for " +
                    "the no.of.classes argument, required INT But found DOUBLE"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension7() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Label is not bool or String");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3," +
                "attribute_4, 'adam', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("[model.target] attribute_4 in " +
                    "updateBayesianClassification should be a STRING or BOOLEAN. But found DOUBLE"));
        } finally {
            siddhiManager.shutdown();
        }

    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension8() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Not implemented optimizer given");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3," +
                "attribute_4, 'adaam', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("model.optimizer should be one of " +
                    "[ADAM, ADAGRAD, SGD, NADAM]. But found adaam"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension9() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - number of classes less than 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 1," +
                "attribute_4, 'adam', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("no.of.classes should be greater than 1. " +
                    "But found 1"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension10() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Target is bool when number " +
                "of classes greater than 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 bool);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3," +
                "attribute_4, 'adam', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("no.of.classes should be 2, if the type " +
                    "of the attribute model.target is BOOLEAN. But found 3"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension11() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Label is of bool type");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 bool );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 2, "
                + "attribute_4, attribute_0, attribute_1 , attribute_2 ,attribute_3)" +
                " insert all events into outputStream;");

        try {


            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {

                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    count.incrementAndGet();
                    EventPrinter.print(inEvents);
                }
            });
            try {
                InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
                siddhiAppRuntime.start();
                inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, true});
                inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, true});
                inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, false});
                inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, false});

                SiddhiTestHelper.waitForEvents(200, 4, count, 1000);

            } catch (Exception e) {
                logger.error(e.getCause().getMessage());
                AssertJUnit.fail();
            } finally {
                siddhiAppRuntime.shutdown();
            }
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.fail("Model fails build with boolean type labels");
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension12() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Wrong parameter order (1)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3," +
                "attribute_4, 0.01, 1, attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Parameter 4 must either be a constant " +
                    "(hyperparameter) or an attribute of the stream (model.features), " +
                    "but found a io.siddhi.core.executor.VariableExpressionExecutor"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension13() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Wrong parameter order (2)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3," +
                "attribute_4, 'adam', 10, attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("4th parameter cannot be type of INT. " +
                    "Only model.sample can be INT, which can be set as the 3th parameter."));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension14() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - duplicated parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3," +
                "attribute_4, 'adam', 'sgd', attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("4th parameter cannot be type of STRING. " +
                    "Only model.optimizer can be STRING, which is already set to ADAM."));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension15() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Negative learning rate given");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1'," +
                "3, attribute_4, -0.01, attribute_0, attribute_1, attribute_2, attribute_3) " +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("learning.rate should be greater than zero. " +
                    "But found -0.010000"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension16() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - number of classes is not int");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 bool, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', "
                + "attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Parameter no.of.classes must be a constant. " +
                    "But found io.siddhi.core.executor.VariableExpressionExecutor"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension17() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - features are integer and double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 int, attribute_2 " +
                "double, attribute_3 double, attribute_4 bool);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 2," +
                "attribute_4, 0.01, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            try {

                InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
                siddhiAppRuntime.start();
                inputHandler.send(new Object[]{0.1, 0.8, 0.21, 0.03, true});
                inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, true});
                inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, false});
                inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, false});

                SiddhiTestHelper.waitForEvents(200, 4, count, 1000);

            } catch (Exception e) {
                logger.error(e.getCause().getMessage());
                AssertJUnit.fail("Model fails to train with integer values");
            } finally {
                siddhiAppRuntime.shutdown();
            }

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.fail("Model fails initialize with integer values");
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension18() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification() \n" +
                "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                    query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid number of parameters [0] for "
                    + "streamingml:updateBayesianClassification"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension19() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - invalid model name");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification(attribute_4, 3,"
                + "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3)"
                + "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Parameter model.name must be a constant. " +
                    "But found io.siddhi.core.executor.VariableExpressionExecutor"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension20() {

        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - invalid model name type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification(0.2, 3," +
                "attribute_4, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid parameter type found for the " +
                    "model.name argument, required STRING But found DOUBLE"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension21() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - incorrect order of parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('m1', 3, " +
                "attribute_4, 1.0, attribute_0, attribute_1, attribute_2, 2)\n"
                + "insert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("8th parameter is not an attribute "
                    + "(VariableExpressionExecutor) present in the stream definition. Found a "
                    + "io.siddhi.core.executor.ConstantExpressionExecutor"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension22() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - more parameters than needed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('m1', 3, " +
                "attribute_4, 10, 'adam', 1.0, attribute_0, attribute_1, attribute_2, attribute_3, 2)" +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Invalid number of parameters for " +
                    "streamingml:updateBayesianClassification. This Stream Processor requires at most 10 parameters, " +
                    "namely, model.name, no.of.classes, model.target, model.samples[optional], " +
                    "model.optimizer[optional], learning.rate[optional], model.features. but found 11 parameters"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension23() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - " +
                "model.target is not an attribute");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 double );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('m1',2," +
                "1.0, attribute_0, attribute_1, attribute_2, attribute_3)" +
                "\ninsert all events into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            AssertJUnit.fail();
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("model.target attribute in "
                    + "updateBayesianClassification should be a variable, but found a "
                    + "io.siddhi.core.executor.ConstantExpressionExecutor"));
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension24() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - model is visible only " +
                "within the SiddhiApp");
        SiddhiManager siddhiManager = new SiddhiManager();

        String trainingStream = "@App:name('BayesianClassificationTestApp1') \n"
                + "define stream StreamTrain (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 string );";
        String trainingQuery = ("@info(name = 'query-train') from " +
                "StreamTrain#streamingml:updateBayesianClassification" + "('model1', 3, attribute_4, 0.1, " +
                "attribute_0, attribute_1, attribute_2, attribute_3) \n" +
                "insert all events into trainOutputStream;\n");

        String inStreamDefinition = "@App:name('BayesianClassificationTestApp2') \n"
                + "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 string );";
        String query = ("@info(name = 'query1') from "
                + "StreamA#streamingml:updateBayesianClassification('model1', 3, attribute_3, 0.1, attribute_0, " +
                "attribute_1, attribute_2) \n" + "insert all events into " + "outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(trainingStream + trainingQuery);
            // should be successful even though both the apps are using the same model name
            // with different feature values
            SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.fail("Model is visible across Siddhi Apps which is wrong!");
        } finally {
            siddhiManager.shutdown();
        }
    }

    @Test
    public void testBayesianClassificationStreamProcessorExtension25() {
        logger.info("BayesianClassificationUpdaterStreamProcessorExtension TestCase - Restore from a restart");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 string);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateBayesianClassification('model1', 3, "
                + "attribute_4, 'nadam', 0.01, attribute_0, attribute_1, attribute_2, attribute_3) \n"
                + "insert all events into outputStream;");

        try {
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
                for (int i = 0; i < 5; i++) {
                    inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
                    inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
                    inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
                    inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
                }

                // persist
                siddhiManager.persist();
                Thread.sleep(5000);

                // send few more events to change the weights
                for (int i = 0; i < 5; i++) {
                    inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});
                    inputHandler.send(new Object[]{4.8, 3.4, 1.9, 0.2, "setosa"});
                }
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
                for (int i = 0; i < 5; i++) {
                    inputHandler.send(new Object[]{5.8, 2.7, 4.1, 1, "versicolor"});
                }
                SiddhiTestHelper.waitForEvents(200, 35, count, 5000);
            } catch (Exception e) {
                logger.error(e.getCause().getMessage());
                AssertJUnit.fail("Model fails train and restore");

            } finally {
                siddhiAppRuntime.shutdown();
            }

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
            AssertJUnit.fail("Model fails to initialize");
        } finally {
            siddhiManager.shutdown();
        }
    }

}
