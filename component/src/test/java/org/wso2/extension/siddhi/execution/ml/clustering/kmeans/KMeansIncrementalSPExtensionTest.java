package org.wso2.extension.siddhi.execution.ml.clustering.kmeans;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import static org.junit.Assert.*;

/**
 * Created by niruhan on 8/14/17.
 */
public class KMeansIncrementalSPExtensionTest {

    private static final Logger logger = Logger.getLogger(KMeansStreamProcessorExtensionTest.class);
    private volatile int count;
    @Before
    public void init() {
        count = 0;
    }

    //Test case for 2D data points with decay rate
    @Test
    public void testClusteringLengthWindow2D_0() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingML:KMeansIncremental('model1', 0.2f, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                /*for (Event event: inEvents) {
                    count++;

                    switch (count) {
                        case 1:
                            Assert.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                        case 2:
                            Assert.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                        case 3:
                            Assert.assertArrayEquals(new Double[]{4.3327, 6.4196}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                    }
                }*/
            }
        });


        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            inputHandler.send(new Object[]{5.7905, 7.7499});
            inputHandler.send(new Object[]{27.458, 23.8848});
            inputHandler.send(new Object[]{3.078, 9.1072});
            inputHandler.send(new Object[]{28.326, 26.7484});
            inputHandler.send(new Object[]{2.2602, 4.6408});
            inputHandler.send(new Object[]{27.3099, 26.1816});
            inputHandler.send(new Object[]{0.9441, 0.6502});
            inputHandler.send(new Object[]{23.9204, 27.6745});
            inputHandler.send(new Object[]{2.0499, 9.9546});
            inputHandler.send(new Object[]{23.7947, 20.8627});
            inputHandler.send(new Object[]{5.8456, 6.8879});
            inputHandler.send(new Object[]{26.7315, 25.5368});
            inputHandler.send(new Object[]{5.8812, 5.9116});
            inputHandler.send(new Object[]{24.5343, 26.77});
            inputHandler.send(new Object[]{4.3866, 0.3132});
            inputHandler.send(new Object[]{22.7654, 25.1381});
            inputHandler.send(new Object[]{7.7824, 9.2299});
            inputHandler.send(new Object[]{23.5167, 24.1244});
            inputHandler.send(new Object[]{5.3086, 9.7503});
            inputHandler.send(new Object[]{25.47, 25.8574});
            inputHandler.send(new Object[]{20.2568, 28.7882});
            inputHandler.send(new Object[]{2.9951, 3.9887});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //Test case for 2D data points without decay rate
    @Test
    public void testClusteringLengthWindow2D_1() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingML:KMeansIncremental('model1', 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                /*for (Event event: inEvents) {
                    count++;

                    switch (count) {
                        case 1:
                            Assert.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                        case 2:
                            Assert.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                        case 3:
                            Assert.assertArrayEquals(new Double[]{4.3327, 6.4196}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                    }
                }*/
            }
        });


        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            inputHandler.send(new Object[]{5.7905, 7.7499});
            inputHandler.send(new Object[]{27.458, 23.8848});
            inputHandler.send(new Object[]{3.078, 9.1072});
            inputHandler.send(new Object[]{28.326, 26.7484});
            inputHandler.send(new Object[]{2.2602, 4.6408});
            inputHandler.send(new Object[]{27.3099, 26.1816});
            inputHandler.send(new Object[]{0.9441, 0.6502});
            inputHandler.send(new Object[]{23.9204, 27.6745});
            inputHandler.send(new Object[]{2.0499, 9.9546});
            inputHandler.send(new Object[]{23.7947, 20.8627});
            inputHandler.send(new Object[]{5.8456, 6.8879});
            inputHandler.send(new Object[]{26.7315, 25.5368});
            inputHandler.send(new Object[]{5.8812, 5.9116});
            inputHandler.send(new Object[]{24.5343, 26.77});
            inputHandler.send(new Object[]{4.3866, 0.3132});
            inputHandler.send(new Object[]{22.7654, 25.1381});
            inputHandler.send(new Object[]{7.7824, 9.2299});
            inputHandler.send(new Object[]{23.5167, 24.1244});
            inputHandler.send(new Object[]{5.3086, 9.7503});
            inputHandler.send(new Object[]{25.47, 25.8574});
            inputHandler.send(new Object[]{20.2568, 28.7882});
            inputHandler.send(new Object[]{2.9951, 3.9887});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //Test case for 2D data points debugging
    @Test
    public void testClusteringLengthWindow2D_2() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingML:KMeansIncremental('model1', 0.2f, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });


        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            inputHandler.send(new Object[]{5.7905, 7.7499});
            inputHandler.send(new Object[]{27.458, 23.8848});
            inputHandler.send(new Object[]{3.078, 9.1072});
            inputHandler.send(new Object[]{28.326, 26.7484});
            inputHandler.send(new Object[]{2.2602, 4.6408});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //Test case for 2D data points of type other than double
    @Test
    public void testClusteringLengthWindow2D_3() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingML:KMeansIncremental('model1', 0.2f, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });


        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            inputHandler.send(new Object[]{5, 7});
            inputHandler.send(new Object[]{27.458f, 23.8848f});
            inputHandler.send(new Object[]{3.078, 9.1072});
            inputHandler.send(new Object[]{"hi", 26.7484});
            inputHandler.send(new Object[]{2.2602, 4.6408});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}