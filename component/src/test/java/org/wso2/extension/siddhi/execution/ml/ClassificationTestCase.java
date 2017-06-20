/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.execution.ml;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;

public class ClassificationTestCase {

    private static final Logger logger = Logger.getLogger(ClassificationTestCase.class);
    private volatile int count;

    @BeforeMethod
    public void init() {
        count = 0;
    }

    @Test
    public void testClassificationStreamProcessorExtension1() throws InterruptedException {
        logger.info("StreamingClasificationStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream inputStream (attribute_0 double, attribute_1 double, attribute_2 "
                + "double,attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from inputStream#ml:classificationHoeffdingtree(5,3,'',"
                + "attribute_0, attribute_1 , attribute_2 ,attribute_3,attribute_4) select att_0 as attribute_0, "
                + "att_1 as attribute_1,att_2 as attribute_2,att_3 as attribute_3, prediction as prediction insert into"
                + " outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (count == 1) {
                    AssertJUnit.assertEquals("setosa", inEvents[0].getData()[4]);
                }
                if (count == 2) {
                    AssertJUnit.assertEquals("versicolor", inEvents[0].getData()[4]);
                }
                if (count == 3) {
                    AssertJUnit.assertEquals("virginica", inEvents[0].getData()[4]);
                }
            }
        });
        Scanner scanner = null;
        try {
            File file = new File("src/test/resources/iris.csv");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            scanner = new Scanner(bufferedReader);

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();
            while (scanner.hasNext()) {
                String eventStr = scanner.nextLine();
                String[] event = eventStr.split(",");
                inputHandler.send(new Object[] { Double.valueOf(event[0]), Double.valueOf(event[1]),
                        Double.valueOf(event[2]), Double.valueOf(event[3]), event[4] });

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            Thread.sleep(1100);
            AssertJUnit.assertEquals(3, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            scanner.close();
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClassificationStreamProcessorExtension2() throws InterruptedException {
        logger.info("StreamingClasificationStreamProcessor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream inputStream (attribute_0 double, attribute_1 double, attribute_2 "
                + "string );";
        String query = ("@info(name = 'query2') from inputStream#ml:classificationHoeffdingtree(3,2,'2,2',1000,"
                + "attribute_0, attribute_1 , attribute_2) select att_0 as attribute_0, att_1 as attribute_1, "
                + "prediction as prediction insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (count == 1) {
                    AssertJUnit.assertEquals("A", inEvents[0].getData()[2]);
                }
                if (count == 2) {
                    AssertJUnit.assertEquals("B", inEvents[0].getData()[2]);
                }
                if (count == 3) {
                    AssertJUnit.assertEquals("A", inEvents[0].getData()[2]);
                }
            }
        });
        Scanner scanner = null;
        try {
            File file = new File("src/test/resources/binary.csv");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            scanner = new Scanner(bufferedReader);

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();
            while (scanner.hasNext()) {
                String eventStr = scanner.nextLine();
                String[] event = eventStr.split(",");
                inputHandler.send(new Object[] { Double.valueOf(event[0]), Double.valueOf(event[1]), event[2] });
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            Thread.sleep(1100);
            AssertJUnit.assertEquals(3, count);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            scanner.close();
            siddhiAppRuntime.shutdown();
        }
    }
}
