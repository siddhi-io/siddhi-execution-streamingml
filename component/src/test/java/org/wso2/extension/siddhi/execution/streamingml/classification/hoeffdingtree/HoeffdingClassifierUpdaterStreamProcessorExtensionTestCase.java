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
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Scanner;


public class HoeffdingClassifierUpdaterStreamProcessorExtensionTestCase {

    private static final Logger logger = Logger
            .getLogger(HoeffdingClassifierUpdaterStreamProcessorExtensionTestCase.class);

    private volatile int count;
    private volatile int accuracy;

    @BeforeMethod
    public void init() {
        count = 0;
        accuracy = 0;
    }


    @Test
    public void testHoeffdingClassifierLearningExtension1() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Assert Model Build with" +
                "default parameters");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, " +
                "prediction as prediction insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, "Trained Successfully"}, inEvents[0]
                            .getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, "Trained Successfully"}, inEvents[0]
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

            Thread.sleep(1100);
            AssertJUnit.assertEquals(12, count);
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
                "from StreamA#streamingml:hoeffdingLearn('model1', 3, 5, 300, 1, 0.05, 0.05, false, false, 2, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, " +
                "prediction as prediction insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, "Trained Successfully"}, inEvents[0]
                            .getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, "Trained Successfully"}, inEvents[0]
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

            Thread.sleep(1100);
            AssertJUnit.assertEquals(5, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testHoeffdingClassifierLearningExtension3() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Features are of any numeric type");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + " attribute_1, attribute_2, attribute_3, prediction as prediction insert into"
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

        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, prediction as prediction insert into"
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

        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, prediction as prediction insert into"
                + " outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
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
            Thread.sleep(1100);
            AssertJUnit.assertEquals(4, count);
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
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 'model1', " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select att_0 as attribute_0, "
                + "att_1 as attribute_1,att_2 as attribute_2,att_3 as attribute_3, prediction as prediction insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Invalid parameter type found for the numberOfClasses " +
                    "argument, required INT but found STRING"));
        }
    }


    @Test
    public void testHoeffdingClassifierLearningExtension7() throws InterruptedException {
        logger.info("HoeffdingClassifierLearningExtension2 TestCase - incorrect initialization");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 int, attribute_4 string );";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select att_0 as attribute_0, "
                + "att_1 as attribute_1,att_2 as attribute_2,att_3 as attribute_3, prediction as prediction insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                    query);
        } catch (Exception e) {
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            logger.error(e);
            AssertJUnit.assertTrue(e.getMessage().contains("Number of classes count must be " +
                    "(ConstantExpressionExecutor) but found org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testHoeffdingClassifierLearningExtension8() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase " +
                "- Accept any numerical type for feature attributes");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 float, attribute_1 double, attribute_2 "
                + "int,attribute_3 long, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, prediction as prediction insert into"
                + " outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, "Trained Successfully"}, inEvents[0]
                            .getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, "Trained Successfully"}, inEvents[0]
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

            Thread.sleep(1100);
            AssertJUnit.assertEquals(5, count);
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

        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, 4, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select att_0 as attribute_0, "
                + "att_1 as attribute_1,att_2 as attribute_2,att_3 as attribute_3, prediction as prediction insert into"
                + " outputStream;");

        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
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

        String inStreamDefinition = "@App:name('HoeffdingTestApp') \n" +
                "define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 " +
                "double, attribute_3 double, attribute_4 String);";
        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select attribute_0, "
                + "attribute_1, attribute_2, attribute_3, prediction insert into"
                + " outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.1, 0.8, 0.2, 0.03, "Trained Successfully"},
                            inEvents[0].getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{0.2, 0.95, 0.22, 0.1, "Trained Successfully"},
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
                    count++;
                    if (count == 4) {
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

            Thread.sleep(1100);
            AssertJUnit.assertEquals(5, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    /*@Test*/
    public void testHoeffdingClassifierLearningExtension11() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Test then train model");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, true, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select att_0 as attribute_0, "
                + "att_1 as attribute_1, att_2 as attribute_2,att_3 as attribute_3, actual_label as actual_label, " +
                "pretrain_prediction as prediction insert into"
                + " outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
                if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.0, 2.2, 4.0, 1.0, "Trained Successfully"}, inEvents[0]
                            .getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, "Trained Successfully"}, inEvents[0]
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

            Thread.sleep(1100);
            AssertJUnit.assertEquals(5, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testHoeffdingClassifierLearningExtension21() throws InterruptedException {
        logger.info("HoeffdingClassifierUpdaterStreamProcessorExtension TestCase - Assert Model Prequntial Evaluation");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream StreamA (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";

        String query = ("@info(name = 'query1') from StreamA#streamingml:hoeffdingLearn('model1', 3, " +
                "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) \n" +
                "insert all events into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(inEvents);
               /* if (count == 1) {
                    AssertJUnit.assertArrayEquals(new Object[]{6, 2.2, 4, 1, "Trained Successfully"}, inEvents[0]
                            .getData());
                }
                if (count == 3) {
                    AssertJUnit.assertArrayEquals(new Object[]{6.9, 3.1, 5.4, 2.1, "Trained Successfully"}, inEvents[0]
                            .getData());
                }*/
            }
        });
    }

    public void testClassificationStreamProcessorExtension2() throws InterruptedException {
        logger.info("StreamingClasificationStreamProcessor Accuracy");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream inputStream (attribute_0 double, attribute_1 double, attribute_2 "
                + "string );";
        String query = ("@info(name = 'query2') from inputStream#ml:multiClassClassification(3,2,'2,2',1000,"
                + "attribute_0, attribute_1 , attribute_2) select att_0 as attribute_0, att_1 as attribute_1, "
                + "prediction as prediction insert into outputStream;");


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });


        Scanner scanner = null;
        try {
            File file = new File("src/test/resources/binary.csv");
            InputStream inputStream = new FileInputStream(file);
            Reader fileReader = new InputStreamReader(inputStream, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            scanner = new Scanner(bufferedReader);

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();
            while (scanner.hasNext()) {
                String eventStr = scanner.nextLine();
                String[] event = eventStr.split(",");
                inputHandler.send(new Object[]{Double.valueOf(event[0]), Double.valueOf(event[1]), event[2]});
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            Thread.sleep(1100);
            //AssertJUnit.assertEquals(3, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            siddhiAppRuntime.shutdown();
        }
    }
}

