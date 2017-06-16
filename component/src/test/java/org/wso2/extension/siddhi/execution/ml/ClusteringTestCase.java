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

import org.testng.AssertJUnit;
import org.apache.log4j.Logger;
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

public class ClusteringTestCase {
    private static final Logger logger = Logger.getLogger(ClusteringTestCase.class);
    private volatile int count;

    @BeforeMethod
    public void init() {
        count = 0;
    }

    @Test
    public void testClusteringStreamProcessorExtension() throws InterruptedException {
        logger.info("StreamingClusteringStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream inputStream (attribute_0 double, "
                + "attribute_1 double,attribute_2 double, attribute_3 double, attribute_4 double );";
        String query = ("@info(name = 'query1') from inputStream#ml:"
                + "clusteringKmeans(5,2,2,1000,10000,attribute_0, attribute_1 , attribute_2 , attribute_3,"
                + " attribute_4) select center0 as center0,center1 as center1 insert into " + "outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                if (count == 1) {
                    AssertJUnit.assertEquals("[26.689013161214312,65.2985408650787,1010.0086638445954,"
                            + "62.56587280444617,436.13502625254046]", inEvents[0].getData()[0]);
                    AssertJUnit.assertEquals("[14.71610078358815,45.70548516901456,1014.585093380316,"
                            + "75.29614297441418,465.57460856962507]", inEvents[0].getData()[1]);
                }
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
        Scanner scanner = null;
        try {

            File file = new File("src/test/resources/ccppTest.csv");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            scanner = new Scanner(bufferedReader);

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();
            while (scanner.hasNext()) {
                String eventStr = scanner.nextLine();
                String[] event = eventStr.split(",");
                inputHandler.send(new Object[] { Double.valueOf(event[0]), Double.valueOf(event[1]),
                        Double.valueOf(event[2]), Double.valueOf(event[3]), Double.valueOf(event[4]) });

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Thread.sleep(1000);
            AssertJUnit.assertEquals(1, count);

        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            scanner.close();
            siddhiAppRuntime.shutdown();
        }
    }
}