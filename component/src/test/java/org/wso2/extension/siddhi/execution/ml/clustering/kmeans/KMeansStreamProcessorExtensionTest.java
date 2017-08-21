package org.wso2.extension.siddhi.execution.ml.clustering.kmeans;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testng.AssertJUnit;
import org.wso2.extension.siddhi.execution.ml.classification.perceptron.PerceptronClassifierUpdaterStreamProcessorExtensionTestCase;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;


public class KMeansStreamProcessorExtensionTest {

    private static final Logger logger = Logger.getLogger(KMeansStreamProcessorExtensionTest.class);
    private volatile int count;
    @Before
    public void init() {
        count = 0;
    }

    //Test case for 2D data points with numberOfEventsToRetrain and decay rate
    @Test
    public void testClusteringLengthWindow2D_0() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model1', 2, 10, 20, 0.2f, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                for (Event event: inEvents) {
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
                }
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

    //Test case for 2D data points with numberOfEventsToRetrain. not decay rate
    @Test
    public void testClusteringLengthWindow2D_1() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model1', 2, 10, 20, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                for (Event event: inEvents) {
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
                }
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

    //Test case for 2D data points with decay rate. not numberOfEventsToRetrain
    @Test
    public void testClusteringLengthWindow2D() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";
        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model1', 2, 10, 0.2f, x, y) " +
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
    //without numberOfEventsToRetrain and decay rate
    @Test
    public void testClusteringLengthWindow2D_2() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model1', 2, 10, x, y) " +
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

    //Test case for 3D data points with k=5
    @Test
    public void testClusteringLengthWindow3D5k() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, z double, timestamp long);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model2', 5, 10, 20, x, y, z) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, closestCentroidCoordinate3 " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                //System.out.println("running receive method");
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
            inputHandler.send(new Object[]{3.0464, 1.6615, 3.2594});
            inputHandler.send(new Object[]{15.7955, 11.1029, 19.3026});
            inputHandler.send(new Object[]{22.5167, 23.678, 26.1105});
            inputHandler.send(new Object[]{37.9662, 38.1719, 39.4197});
            inputHandler.send(new Object[]{45.7562, 40.927, 49.8103});
            inputHandler.send(new Object[]{7.8709, 1.814, 3.7995});
            inputHandler.send(new Object[]{18.112, 15.1656, 13.7341});
            inputHandler.send(new Object[]{27.2222, 25.221, 24.9664});
            inputHandler.send(new Object[]{34.6105, 30.6252, 37.9087});
            inputHandler.send(new Object[]{47.414, 43.9892, 42.7132});
            inputHandler.send(new Object[]{7.8421, 7.1038, 0.8666});
            inputHandler.send(new Object[]{10.3762, 12.5236, 18.6183});
            inputHandler.send(new Object[]{22.9102, 22.492, 20.2486});
            inputHandler.send(new Object[]{30.0185, 30.4046, 32.6397});
            inputHandler.send(new Object[]{45.8596, 41.6197, 48.9654});
            inputHandler.send(new Object[]{6.4989, 5.9532, 6.9627});
            inputHandler.send(new Object[]{19.098, 19.6884, 10.5624});
            inputHandler.send(new Object[]{29.5058, 26.2186, 23.2569});
            inputHandler.send(new Object[]{31.504, 35.2271, 30.861});
            inputHandler.send(new Object[]{43.8162, 42.5516, 42.1917});
            inputHandler.send(new Object[]{6.802, 5.622, 0.0968});
            inputHandler.send(new Object[]{14.9855, 14.9271, 14.6778});
            inputHandler.send(new Object[]{22.8387, 29.0477, 23.7825});
            inputHandler.send(new Object[]{31.7093, 30.9383, 34.1084});
            inputHandler.send(new Object[]{48.5657, 46.8033, 47.294});
            inputHandler.send(new Object[]{2.7435, 5.0484, 8.3643});
            inputHandler.send(new Object[]{16.7214, 16.5791, 16.4248});
            inputHandler.send(new Object[]{29.3467, 26.5279, 24.4627});
            inputHandler.send(new Object[]{37.1945, 39.3291, 30.9883});
            inputHandler.send(new Object[]{49.974, 43.075, 47.598});
            inputHandler.send(new Object[]{3.8805, 6.7386, 3.6699});
            inputHandler.send(new Object[]{11.6763, 19.3813, 14.618});
            inputHandler.send(new Object[]{26.1402, 25.0853, 24.304});
            inputHandler.send(new Object[]{33.5385, 31.3817, 36.6539});
            inputHandler.send(new Object[]{49.5696, 41.3806, 45.8867});
            inputHandler.send(new Object[]{7.615, 9.0716, 6.7704});
            inputHandler.send(new Object[]{17.4627, 13.3232, 10.7396});
            inputHandler.send(new Object[]{26.1163, 27.9993, 29.4323});
            inputHandler.send(new Object[]{30.1437, 36.8126, 35.784});
            inputHandler.send(new Object[]{43.5106, 41.1323, 44.9021});
            inputHandler.send(new Object[]{0.8561, 9.8248, 3.7628});
            inputHandler.send(new Object[]{19.8792, 17.9442, 17.8631});
            inputHandler.send(new Object[]{24.351, 25.887, 20.1706});
            inputHandler.send(new Object[]{36.236, 36.9917, 38.3142});
            inputHandler.send(new Object[]{44.3563, 43.4616, 43.8337});
            inputHandler.send(new Object[]{1.2693, 5.6595, 6.9171});
            inputHandler.send(new Object[]{15.9958, 13.9021, 17.3244});
            inputHandler.send(new Object[]{23.2278, 26.695, 29.6055});
            inputHandler.send(new Object[]{36.4687, 35.8668, 38.6454});
            inputHandler.send(new Object[]{44.3819, 42.4329, 43.5167});
            inputHandler.send(new Object[]{3.0464, 1.6615, 3.2594});
            inputHandler.send(new Object[]{15.7955, 11.1029, 19.3026});
            inputHandler.send(new Object[]{22.5167, 23.678, 26.1105});
            inputHandler.send(new Object[]{37.9662, 38.1719, 39.4197});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //Test case for 2D data points with k=3
    @Test
    public void testClusteringLengthWindow2D3k() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, timestamp long);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model1', 3, 10, 30, 0.2f, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2 " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event: inEvents) {
                    count++;

                    switch (count) {
                        case 1:
                            Assert.assertArrayEquals(new Double[]{45.9602, 44.4844}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                        case 2:
                            Assert.assertArrayEquals(new Double[]{4.6481, 6.0874}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                        case 3:
                            Assert.assertArrayEquals(new Double[]{25.2928, 26.4717}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start();
        try {
            inputHandler.send(new Object[]{8.6595, 9.2534});
            inputHandler.send(new Object[]{26.91, 27.7194});
            inputHandler.send(new Object[]{40.9496, 46.751});
            inputHandler.send(new Object[]{3.0379, 9.381});
            inputHandler.send(new Object[]{24.0843, 22.5933});
            inputHandler.send(new Object[]{46.757, 44.8402});
            inputHandler.send(new Object[]{6.0502, 4.7133});
            inputHandler.send(new Object[]{23.9084, 29.9808});
            inputHandler.send(new Object[]{44.3852, 45.5709});
            inputHandler.send(new Object[]{2.2483, 4.592});
            inputHandler.send(new Object[]{26.5544, 29.301});
            inputHandler.send(new Object[]{49.4444, 43.9536});
            inputHandler.send(new Object[]{4.6758, 5.1065});
            inputHandler.send(new Object[]{28.7405, 29.5637});
            inputHandler.send(new Object[]{41.1196, 45.267});
            inputHandler.send(new Object[]{9.8962, 2.051});
            inputHandler.send(new Object[]{20.8293, 26.0885});
            inputHandler.send(new Object[]{49.3169, 41.8329});
            inputHandler.send(new Object[]{1.3745, 8.1585});
            inputHandler.send(new Object[]{27.303, 21.9864});
            inputHandler.send(new Object[]{47.0754, 46.1605});
            inputHandler.send(new Object[]{3.707, 7.4449});
            inputHandler.send(new Object[]{22.264, 24.8863});
            inputHandler.send(new Object[]{45.068, 40.2443});
            inputHandler.send(new Object[]{5.9062, 7.9719});
            inputHandler.send(new Object[]{29.9257, 25.5476});
            inputHandler.send(new Object[]{49.2968, 44.2055});
            inputHandler.send(new Object[]{0.9251, 2.2017});
            inputHandler.send(new Object[]{22.4079, 27.0502});
            inputHandler.send(new Object[]{46.1892, 46.0184});
            inputHandler.send(new Object[]{8.6595, 9.2534});
            inputHandler.send(new Object[]{26.91, 27.7194});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    //Test case for 10D data points
    @Test
    public void testClusteringLengthWindow10D() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x1 double, x2 double, x3 double, x4 double, x5 double, x6 double, x7 double, x8 double, x9 double, x10 double, timestamp long);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#window.length(5)#streamingml:kmeans('model4', 3, 10, 30, 0.3f, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2,closestCentroidCoordinate3, closestCentroidCoordinate4, closestCentroidCoordinate5, closestCentroidCoordinate6, closestCentroidCoordinate7, closestCentroidCoordinate8, closestCentroidCoordinate9, closestCentroidCoordinate10 " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event: inEvents) {
                    count++;

                    switch (count) {
                        case 1:
                            Assert.assertArrayEquals(new Double[]{45.2486, 45.2961, 45.0537, 44.8475, 45.2422, 44.4798, 44.8825, 44.1090, 45.1104, 44.5099}, new Object[]{event.getData(0), event.getData(1),event.getData(2),event.getData(3),event.getData(4),event.getData(5),event.getData(6),event.getData(7),event.getData(8),event.getData(9)});
                            break;
                        case 2:
                            Assert.assertArrayEquals(new Double[]{6.4391, 5.2055, 4.858, 5.0139, 4.9563, 4.953, 4.6498, 6.024, 4.7561, 5.4413}, new Object[]{event.getData(0), event.getData(1),event.getData(2),event.getData(3),event.getData(4),event.getData(5),event.getData(6),event.getData(7),event.getData(8),event.getData(9)});
                            break;
                        case 3:
                            Assert.assertArrayEquals(new Double[]{24.8115, 25.9062, 25.1077, 26.3784, 26.01, 24.2884, 24.6903, 24.3359, 23.6244, 24.9334}, new Object[]{event.getData(0), event.getData(1),event.getData(2),event.getData(3),event.getData(4),event.getData(5),event.getData(6),event.getData(7),event.getData(8),event.getData(9)});
                            break;
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start();
        try {
            inputHandler.send(new Object[]{8.4006, 0.6045, 1.0634, 2.5923, 0.2909, 6.0318, 5.2194, 8.1952, 6.5925, 0.0644});
            inputHandler.send(new Object[]{25.1833, 26.2168, 23.5599, 29.7557, 29.9423, 25.1303, 26.6528, 21.2207, 21.0566, 28.8507});
            inputHandler.send(new Object[]{41.5751, 48.3917, 41.3757, 44.3261, 40.2119, 41.1496, 48.6898, 41.583, 46.836, 42.3766});
            inputHandler.send(new Object[]{6.5068, 2.9903, 9.293, 1.3811, 4.0006, 1.1389, 5.9239, 1.8798, 3.088, 0.6718});
            inputHandler.send(new Object[]{24.0418, 22.41, 28.9318, 26.9783, 20.3859, 26.8972, 27.7941, 21.8818, 20.3053, 20.9119});
            inputHandler.send(new Object[]{43.2847, 42.4547, 45.561, 41.3031, 43.7302, 44.7738, 40.2929, 40.5572, 40.7004, 47.9038});
            inputHandler.send(new Object[]{5.7655, 9.3944, 2.0042, 5.6881, 3.8246, 8.721, 3.2686, 3.6687, 2.9914, 5.9546});
            inputHandler.send(new Object[]{29.5688, 29.0483, 24.5111, 24.5728, 24.6466, 20.6774, 22.3055, 22.5611, 23.9892, 25.3404});
            inputHandler.send(new Object[]{44.6656, 41.9337, 46.2645, 47.7455, 44.1161, 42.3892, 43.1119, 49.7757, 40.7597, 40.1814});
            inputHandler.send(new Object[]{3.6982, 3.0602, 0.0822, 7.3049, 9.407, 8.2044, 7.2494, 9.4857, 7.5458, 7.3018});
            inputHandler.send(new Object[]{28.3939, 28.174, 29.7013, 26.0018, 24.5075, 23.2057, 21.4671, 23.6484, 21.7164, 20.3571});
            inputHandler.send(new Object[]{43.1037, 43.3149, 46.3058, 48.6343, 49.5634, 42.2124, 42.9253, 48.4812, 40.3779, 42.9361});
            inputHandler.send(new Object[]{8.1228, 6.8373, 0.9589, 4.0101, 9.3271, 3.1457, 4.5484, 3.1469, 4.2506, 4.4189});
            inputHandler.send(new Object[]{20.8421, 25.2925, 24.5996, 24.2815, 28.1304, 27.714, 27.2867, 25.5283, 21.788, 28.8521});
            inputHandler.send(new Object[]{42.2389, 49.0652, 48.8715, 43.4585, 44.7427, 46.8175, 46.5949, 40.2362, 42.4705, 43.1794});
            inputHandler.send(new Object[]{1.6499, 1.6105, 2.5832, 9.2161, 9.5054, 1.4102, 3.7265, 6.4077, 9.6987, 6.7666});
            inputHandler.send(new Object[]{20.3057, 26.6666, 22.0666, 24.6667, 22.8689, 25.1348, 21.5598, 24.7805, 22.1504, 29.8942});
            inputHandler.send(new Object[]{49.9572, 45.6773, 43.2645, 46.1552, 46.3137, 49.4851, 45.265, 43.8225, 48.2223, 44.9731});
            inputHandler.send(new Object[]{7.8334, 9.5602, 8.4482, 3.6483, 0.2906, 1.0926, 3.3095, 1.8523, 4.0894, 6.4947});
            inputHandler.send(new Object[]{25.5494, 25.9623, 20.7722, 27.8391, 28.3965, 20.2748, 20.2401, 29.9752, 20.6588, 25.3466});
            inputHandler.send(new Object[]{45.8195, 43.2914, 41.5843, 42.3883, 49.1927, 45.1339, 47.6554, 44.1657, 44.8267, 42.2404});
            inputHandler.send(new Object[]{6.3312, 6.8922, 8.292, 6.6083, 5.8872, 1.9287, 7.1752, 8.5791, 6.2261, 6.461});
            inputHandler.send(new Object[]{21.6857, 24.8914, 26.8182, 28.1578, 27.898, 29.31, 28.5743, 29.0051, 26.2639, 20.8181});
            inputHandler.send(new Object[]{49.5384, 41.7407, 44.44, 48.6279, 40.0091, 44.838, 43.7001, 40.2115, 49.7994, 43.4042});
            inputHandler.send(new Object[]{9.628, 5.2659, 7.89, 0.1343, 1.9687, 9.7142, 2.3588, 9.0443, 1.0967, 9.6459});
            inputHandler.send(new Object[]{23.777, 29.6507, 25.1962, 29.0858, 29.6105, 21.6832, 22.5473, 21.9967, 29.7306, 28.3137});
            inputHandler.send(new Object[]{44.2324, 49.7846, 45.7918, 43.9529, 46.2282, 45.4589, 46.5645, 48.7078, 47.3368, 49.6748});
            inputHandler.send(new Object[]{6.4548, 5.8396, 7.9648, 9.5553, 5.0611, 8.1426, 3.7187, 7.9803, 1.9818, 6.6329});
            inputHandler.send(new Object[]{28.7674, 20.7496, 24.9205, 22.444, 23.713, 22.8569, 28.4749, 22.761, 28.5845, 20.6494});
            inputHandler.send(new Object[]{48.0704, 47.3068, 47.0782, 41.8831, 48.3138, 42.5396, 44.0249, 43.5495, 49.7745, 48.2287});
            inputHandler.send(new Object[]{8.4006, 0.6045, 1.0634, 2.5923, 0.2909, 6.0318, 5.2194, 8.1952, 6.5925, 0.0644});
            inputHandler.send(new Object[]{25.1833, 26.2168, 23.5599, 29.7557, 29.9423, 25.1303, 26.6528, 21.2207, 21.0566, 28.8507});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //Acceptance Criteria for mini batch
    //System should validate that in the InputStream except for attribute_0 and attribute_1 others should be constant throughout the stream
    @Test
    public void testAC0() throws Exception {
        logger.info("KMeansStreamProcessorExtension Test case - model name is variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, z String);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans(z, 2, 10, 20, 0.2f, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
                AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
                AssertJUnit.assertTrue(e.getMessage().contains("modelName has to be a constant., when creating query query1 in siddhi app"));
        }
    }

    @Test
    public void testAC1() throws Exception {
        logger.info("KMeansStreamProcessorExtension Test case - numberOfClusters(k) is variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, numberOfClusters int);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model_1', numberOfClusters, 10, 20, 0.2f, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            //e.printStackTrace();
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("k has to be a constant., when creating query query1 in siddhi app"));
        }
    }

    @Test
    public void testAC2() throws Exception {
        logger.info("KMeansStreamProcessorExtension Test case - maxIterations is variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, maxIterations int);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model_1', 2, maxIterations, 20, 0.2f, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            //e.printStackTrace();
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Maximum iterations has to be a constant., when creating query query1 in siddhi app"));
        }
    }

    @Test
    public void testAC3() throws Exception {
        logger.info("KMeansStreamProcessorExtension Test case - numberOfEventsToRetrain is variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, numberOfEventsToRetrain int);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model_1', 2, 10, numberOfEventsToRetrain, 0.2f, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            //e.printStackTrace();
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("numberOfEventsToRetrain has to be a constant, when creating query query1 in siddhi app"));
        }
    }

    @Test
    public void testAC4() throws Exception {
        logger.info("KMeansStreamProcessorExtension Test case - decayRate is variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, decayRate float);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kmeans('model_1', 2, 10, 20, decayRate, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            //e.printStackTrace();
            AssertJUnit.assertTrue(e instanceof SiddhiAppValidationException);
            AssertJUnit.assertTrue(e.getMessage().contains("Decay rate has to be a constant, when creating query query1 in siddhi app"));
        }
    }

}