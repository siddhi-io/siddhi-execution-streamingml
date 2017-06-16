package org.wso2.extension.siddhi.execution.ml;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;


public class ClusteringKMeansTestCase {

    private volatile int count;
    @Before
    public void init() {
        count = 0;

    }

    @Test
    public void testClusteringLengthWindow() throws Exception {

        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (value double, timestamp long);";

        String executionPlan = ("@info(name = 'query1') "
                  + "from InputStream#window.length(5)#kmeans:cluster(value, 4, 20, 5) "
                  + "select value, matchedClusterCentroid, matchedClusterIndex, distanceToCenter "
                  + "insert into OutputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inputStream + executionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
               EventPrinter.print(timeStamp, inEvents, removeEvents);
               // int count = 0;
                for (Event event : inEvents) {
                    count++;
                 //System.out.println(event);
                    switch (count) {
                        case 1:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 2:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 3:
                            Assert.assertEquals(3.06, event.getData(1));
                            break;
                        case 4:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 5:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 6:
                            Assert.assertEquals(3.13, event.getData(1));
                            break;
                        case 7:
                            Assert.assertEquals(3.045,event.getData(1));
                        default:
                            break;
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{3.08});//1
        inputHandler.send(new Object[]{3.02});//2
        inputHandler.send(new Object[]{3.22});//3
        inputHandler.send(new Object[]{3.06});//4
        inputHandler.send(new Object[]{3.0});//5
        inputHandler.send(new Object[]{3.03});//6
        inputHandler.send(new Object[]{3.06});//7
        inputHandler.send(new Object[]{2.92});//8
        inputHandler.send(new Object[]{2.99});//9
        inputHandler.send(new Object[]{3.13});//10
        inputHandler.send(new Object[]{3.08});//11


    }



    @Test
    public void testClusteringTimeWindow() throws Exception {

        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (value double, timestamp long);";

        String executionPlan = ("@info(name = 'query1') "
                + "from InputStream#window.time(5 sec)#kmeans:cluster(value, 4, 20, 5) "
                + "select value, matchedClusterCentroid, matchedClusterIndex, distanceToCenter "
                + "insert into OutputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inputStream + executionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                // int count = 0;
                for (Event event : inEvents) {
                    // countDownLatch.countDown();
                    count++;
                    //System.out.println(event);
                    switch (count) {
                        case 1:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 2:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 3:
                            Assert.assertEquals(3.06, event.getData(1));
                            break;
                        case 4:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 5:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 6:
                            Assert.assertEquals(3.13, event.getData(1));
                            break;
                        case 7:
                            Assert.assertEquals(3.045,event.getData(1));
                        default:
                            break;
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{3.08});//1
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.02});//2
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.22});//3
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.06});//4
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.0});//5
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.03});//6
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.06});//7
        Thread.sleep(1000);
        inputHandler.send(new Object[]{2.92});//8
        Thread.sleep(1000);
        inputHandler.send(new Object[]{2.99});//9
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.13});//10
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.08});//11
        Thread.sleep(1000);


    }



    @Test
    public void testClusteringBatchWindow() throws Exception{
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (value double, timestamp long);";

        String executionPlan = ("@info(name = 'query1') "
                + "from InputStream#window.timeBatch(5 sec)#kmeans:cluster(value, 4, 20, 5) "
                + "select value, matchedClusterCentroid, matchedClusterIndex, distanceToCenter "
                + "insert into OutputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inputStream + executionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                // int count = 0;
                for (Event event : inEvents) {
                    // countDownLatch.countDown();
                    count++;
                  //  System.out.println(event);
                    switch (count) {
                        case 1:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 2:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 3:
                            Assert.assertEquals(3.06, event.getData(1));
                            break;
                        case 4:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 5:
                            Assert.assertEquals(3.01, event.getData(1));
                            break;
                        case 6:
                            Assert.assertEquals(3.13, event.getData(1));
                            break;
                        default:
                            break;
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{3.08});//1
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.02});//2
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.22});//3
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.06});//4
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.0});//5
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.03});//6
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.06});//7
        Thread.sleep(1000);
        inputHandler.send(new Object[]{2.92});//8
        Thread.sleep(1000);
        inputHandler.send(new Object[]{2.99});//9
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.13});//10
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.08});//11
        Thread.sleep(1000);
        inputHandler.send(new Object[]{2.92});//12
        Thread.sleep(1000);
        inputHandler.send(new Object[]{2.99});//13
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.13});//14
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3.08});//15
        Thread.sleep(1000);
    }



    @Test
    public void clusteringDiscontinueTrainingTestcase() throws Exception{
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (value double, train bool);";

        String executionPlan = ("@info(name = 'query1') "
                + "from InputStream#window.length(10)#kmeans:cluster(value, 4, 20, train) "
                + "select value, matchedClusterCentroid, matchedClusterIndex, distanceToCenter "
                + "insert into OutputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inputStream + executionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                // int count = 0;
                for (Event event : inEvents) {
                    // countDownLatch.countDown();
                    count++;
                  // System.out.println(event);
                    switch (count) {
                        case 1:
                            Assert.assertEquals(3.13, event.getData(1));
                            break;
                        case 2:
                            Assert.assertEquals(3.05, event.getData(1));
                            break;
                        case 3:
                            Assert.assertEquals(3.05, event.getData(1));
                            break;
                        case 4:
                            Assert.assertEquals(2.97, event.getData(1));
                            break;
                        case 5:
                            Assert.assertEquals(2.97, event.getData(1));
                            break;
                        case 6:
                            Assert.assertEquals(3.13, event.getData(1));
                            break;
                        default:
                            break;
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{3.08,false});//1
        inputHandler.send(new Object[]{3.02,false});//2
        inputHandler.send(new Object[]{3.22,false});//3
        inputHandler.send(new Object[]{3.06,false});//4
        inputHandler.send(new Object[]{3.0, false});//5
        inputHandler.send(new Object[]{3.03,false});//6
        inputHandler.send(new Object[]{3.06,false});//7
        inputHandler.send(new Object[]{2.92,false});//8
        inputHandler.send(new Object[]{2.99,false});//9
        inputHandler.send(new Object[]{3.13,true});//10
        inputHandler.send(new Object[]{3.08,false});//11
        inputHandler.send(new Object[]{3.06,false});//12
        inputHandler.send(new Object[]{2.92,false});//13
        inputHandler.send(new Object[]{2.99,false});//14
        inputHandler.send(new Object[]{3.13,false});//15
    }


    }

