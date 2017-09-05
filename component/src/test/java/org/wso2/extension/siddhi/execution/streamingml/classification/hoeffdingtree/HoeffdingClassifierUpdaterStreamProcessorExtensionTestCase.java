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
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.concurrent.atomic.AtomicInteger;


public class HoeffdingClassifierUpdaterStreamProcessorExtensionTestCase {

    private static final Logger logger = Logger
            .getLogger(HoeffdingClassifierUpdaterStreamProcessorExtensionTestCase.class);

    private AtomicInteger count;

    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }

    @Test
    public void testHoeffdingClassifierLearningExtension1() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Assert Model Build with" +
                "default parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, " +
                "accuracy insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, 0.0}, inEvents[0]
                            .getData());
                }
                if (count.get() == 6) {
                    AssertJUnit.assertArrayEquals(new Object[]{4.8, 3.4, 1.9, 0.2, 0.333}, inEvents[0]
                            .getData());
                }
                if (count.get() == 7) {
                    AssertJUnit.assertArrayEquals(new Object[]{5.8, 2.7, 4.1, 1, 0.5}, inEvents[0]
                            .getData());
                }
            }
        });

        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
            inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
            inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
            inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});
            inputHandler.send(new Object[]{4.8, 3.4, 1.9, 0.2, "setosa"});
            inputHandler.send(new Object[]{5.8, 2.7, 4.1, 1, "versicolor"});

            SiddhiTestHelper.waitForEvents(200, 7, count, 60000);

        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension2() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Assert model build " +
                "with manual configurations");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:updateHoeffdingTree('model1', 3, 5, 300, 1e-7, 0.05, false, false, 2, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, " +
                "accuracy insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, 0.0}, inEvents[0]
                            .getData());
                }
                if (count.get() == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, 0.0}, inEvents[0]
                            .getData());
                }
            }
        });

        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
            inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
            inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
            inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});

            SiddhiTestHelper.waitForEvents(200, 5, count, 60000);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testHoeffdingClassifierLearningExtension3() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Features are of un-supported type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + " attribute_1, attribute_2, attribute_3, accuracy insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("6th parameter is not an double type attribute. " +
                    "Check the number of attribute entered as a attribute set with number of attribute " +
                    "configuration parameter, when creating query"));
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension4() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Label is not of type string or " +
                "bool");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 int );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, accuracy insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("[label attribute] in 6 th index of classifierUpdate " +
                    "should be either a BOOL or a STRING but found INT"));
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension5() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Label is of bool type");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 bool );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 2, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, accuracy insert into"
                + " outputStream;");

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

            SiddhiTestHelper.waitForEvents(200, 4, count, 60000);

        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testHoeffdingClassifierLearningExtension6() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - number of classes is not int");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 'model1', " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select att_0 as attribute_0, "
                + "att_1 as attribute_1,att_2 as attribute_2,att_3 as attribute_3, accuracy insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found for "
                    + "the number_of_classes argument, required INT but found STRING"));
        }
    }


    @Test
    public void testHoeffdingClassifierLearningExtension7() throws InterruptedException {
        logger.info("HoeffdingClassifierLearningExtension2 TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select att_0 as attribute_0, "
                + "att_1 as attribute_1,att_2 as attribute_2,att_3 as attribute_3, accuracy insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                    query);
        } catch (Exception e) {
            logger.error(e);
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Number of classes must be (ConstantExpressionExecutor) "
                    + "but found org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension8() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Accept any numerical type for feature attributes");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 float, attribute_1 double, attribute_2 "
                + "int,attribute_3 long, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 3, "
                + "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, accuracy insert into"
                + " outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, 0.0}, inEvents[0]
                            .getData());
                }
                if (count.get() == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, 0.0}, inEvents[0]
                            .getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
            inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
            inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
            inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});

            SiddhiTestHelper.waitForEvents(200, 5, count, 60000);

        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testHoeffdingClassifierLearningExtension9() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - number of parameters not accepted");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 3, 4, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, accuracy insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Number of hyper-parameters needed for model manual " +
                    "configuration is 7 but found 1"));
        }
    }

    //Restore from a restart
    @Test
    public void testHoeffdingClassifierLearningExtension10() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Restore from a restart");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());

        String inStreamDefinition = "@App:name('HoeffdingTestApp') \n"
                + "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double, attribute_3 double, attribute_4 String);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 2, "
                + "attribute_0, attribute_1 , attribute_2 , attribute_3, attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, accuracy insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, 0.0},
                            inEvents[0].getData());
                }
                if (count.get() == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.2, 0.95, 0.22, 0.1, 1.0},
                            inEvents[0].getData());
                }
            }
        });
        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{0.1, 0.8, 0.2, 0.03, true});
            // persist
            siddhiManager.persist();
            Thread.sleep(5000);
            // send few more events to change the weights
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, false});
            inputHandler.send(new Object[]{0.2, 0.95, 0.22, 0.1, true});
            inputHandler.send(new Object[]{0.75, 0.1, 0.58, 0.71, false});
            Thread.sleep(1000);
            // shutdown the app
            siddhiAppRuntime.shutdown();

            // recreate the same app
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {

                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    count.incrementAndGet();
                    if (count.get() == 4) {
                        // weights should be restored and should be following
                        AssertJUnit.assertArrayEquals(new Object[]{0.001, 0.008, 0.002, 3.0E-4}, inEvents[0]
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
            inputHandler.send(new Object[]{0.8, 0.1, 0.65, 0.92, false});

            SiddhiTestHelper.waitForEvents(200, 5, count, 60000);

        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension11() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Hyperparameter not type ConstantExecutor");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 3, 5, 200, " +
                "attribute_0, 0.05, false, false, 2, attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4)" +
                " select attribute_0, attribute_1, attribute_2, attribute_3, accuracy insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Allowable Split Error must be " +
                    "(ConstantExpressionExecutor) but found org.wso2.siddhi.core.executor.VariableExpressionExecutor" +
                    " in position 5"));
        }
    }


    @Test
    public void testHoeffdingClassifierLearningExtension12() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Assert Model Prequntial Evaluation");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) \n" +
                "select attribute_0, attribute_1, attribute_2, attribute_3, accuracy insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                count.incrementAndGet();
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, 0.0}, inEvents[0]
                            .getData());
                }
                if (count.get() == 6) {
                    AssertJUnit.assertArrayEquals(new Object[]{4.8, 3.4, 1.9, 0.2, 0.333}, inEvents[0]
                            .getData());
                }
                if (count.get() == 7) {
                    AssertJUnit.assertArrayEquals(new Object[]{5.8, 2.7, 4.1, 1, 0.5}, inEvents[0]
                            .getData());
                }
            }
        });

        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
            inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
            inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
            inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});
            inputHandler.send(new Object[]{4.8, 3.4, 1.9, 0.2, "setosa"});
            inputHandler.send(new Object[]{5.8, 2.7, 4.1, 1, "versicolor"});
            inputHandler.send(new Object[]{5.1, 2.5, 3, 1.1, "versicolor"});
            inputHandler.send(new Object[]{6.3, 2.8, 5.1, 1.5, "virginica"});
            inputHandler.send(new Object[]{5.1, 3.8, 1.6, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.5, 2.8, 4.6, 1.5, "versicolor"});
            inputHandler.send(new Object[]{5.7, 2.5, 5, 2, "virginica"});

            SiddhiTestHelper.waitForEvents(200, 12, count, 60000);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension13() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - no label is passed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 , attribute_3) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, accuracy insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("[label attribute] in 5 th index of classifierUpdate " +
                    "should be either a BOOL or a STRING but found DOUBLE"));
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension14() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - No_Of_Classes is not passed");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double);";

        String query = ("@info(name = 'query1') from StreamA#streamingml:updateHoeffdingTree('model1', " +
                "attribute_0, attribute_1 , attribute_2 , attribute_3) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, accuracy insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Number of classes must be (ConstantExpressionExecutor)" +
                    " but found org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension15() throws InterruptedException {
        logger.info("HoeffdingClassifierStreamProcessorExtension TestCase - model is visible only within the " +
                "SiddhiApp");
        SiddhiManager siddhiManager = new SiddhiManager();

        String trainingStream1 = "@App:name('HoeffdingTestApp') \n" +
                "define stream StreamTrain (attribute_0 double, " +
                "attribute_1 double, attribute_2 double, attribute_3 double, attribute_4 string );";
        String trainingQuery1 = ("@info(name = 'query-train') " +
                "from StreamTrain#streamingml:updateHoeffdingTree('ml', 4, " +
                "attribute_0, attribute_1, attribute_2, attribute_3, attribute_4) \n"
                + "insert all events into trainOutputStream;\n");

        String trainingStrream2 = "@App:name('HoeffdingTreeTestApp') define stream StreamTrain (attribute_0 double, "
                + "attribute_1 double, attribute_2 double,attribute_3 string );";
        String trainingQuery2 = ("@info(name = 'query-train') "
                + "from StreamTrain#streamingml:updateHoeffdingTree('ml', 3, "
                + "attribute_0, attribute_1 , attribute_2 ,attribute_3) select attribute_0, "
                + "attribute_1, attribute_2, accuracy insert into outputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager
                    .createSiddhiAppRuntime(trainingStream1 + trainingQuery1);
            // should be successful even though both the apps are using the same model name with different feature
            // values
            SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager
                    .createSiddhiAppRuntime(trainingStrream2 + trainingQuery2);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.fail("Model is visible across Siddhi Apps which is wrong!");
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension16() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Configure a model with 'binary_split' set to true");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:updateHoeffdingTree('model1', 3, 5, 300, 1e-7, 0.05, true, false, 2, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, " +
                "accuracy insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, 0.0}, inEvents[0]
                            .getData());
                }
                if (count.get() == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, 0.0}, inEvents[0]
                            .getData());
                }
            }
        });

        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
            inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
            inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
            inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});

            SiddhiTestHelper.waitForEvents(200, 5, count, 60000);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension17() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Configure a model with prepruning disabled");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:updateHoeffdingTree('model1', 3, 5, 300, 1e-7, 0.05, false, true, 2, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, " +
                "accuracy insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count.incrementAndGet();
                EventPrinter.print(inEvents);
                if (count.get() == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, 0.0}, inEvents[0]
                            .getData());
                }
                if (count.get() == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, 0.0}, inEvents[0]
                            .getData());
                }
            }
        });

        try {
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StreamA");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{6, 2.2, 4, 1, "versicolor"});
            inputHandler.send(new Object[]{5.4, 3.4, 1.7, 0.2, "setosa"});
            inputHandler.send(new Object[]{6.9, 3.1, 5.4, 2.1, "virginica"});
            inputHandler.send(new Object[]{4.3, 3, 1.1, 0.1, "setosa"});
            inputHandler.send(new Object[]{6.1, 2.8, 4.7, 1.2, "versicolor"});

            SiddhiTestHelper.waitForEvents(200, 5, count, 60000);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension18() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Configure a model with tie threshold value >1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:updateHoeffdingTree('model1', 3, 5, 300, 1e-7, 2, false, false, 2, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, " +
                "accuracy insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Option tieThreshold cannot be greater than 1.0, "
                    + "out of range: 2.0"));
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension19() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Configure a model with tie threshold value <0");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') " +
                "from StreamA#streamingml:updateHoeffdingTree('model1', 3, 5, 300, 1e-7, -2, false, false, 2, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, " +
                "accuracy insert into outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            logger.error(e.getMessage());
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Option tieThreshold cannot be less than 0.0,"
                    + " out of range: -2.0"));
        }
    }


}

