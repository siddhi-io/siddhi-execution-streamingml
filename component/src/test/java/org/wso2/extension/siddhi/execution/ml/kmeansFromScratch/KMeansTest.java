package org.wso2.extension.siddhi.execution.ml.kmeansFromScratch;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.stream.input.InputHandler;

import static org.junit.Assert.*;

/**
 * Created by niruhan on 7/18/17.
 */
public class KMeansTest {

    private volatile int count;
    @Before
    public void init() {
        count = 0;
    }

    //Test case for 2D data points
    @Test
    public void testClusteringLengthWindow2D() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, timestamp long);";

        String query = (
                "@info(name = 'query1') " +
                "from InputStream#kmeans:cluster(2, 10, 20, x, y) " +
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
                            Assert.assertArrayEquals(new Double[]{25.3023, 24.9005}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                        case 2:
                            Assert.assertArrayEquals(new Double[]{4.3103, 5.2955}, new Object[]{event.getData(0), event.getData(1)});
                            break;
                    }
                }*/
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start();
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

    //Test case for 3D data points
    @Test
    public void testClusteringLengthWindow2D3k() throws Exception {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, timestamp long);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#kmeans:cluster(3, 10, 30, 0.2f, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2 " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
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
                        "from InputStream#window.length(5)#kmeans:cluster(3, 10, 10, 0.3f, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) " +
                        "select x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, closestCentroidCoordinate2,closestCentroidCoordinate3, closestCentroidCoordinate4, closestCentroidCoordinate5, closestCentroidCoordinate6, closestCentroidCoordinate7, closestCentroidCoordinate8, closestCentroidCoordinate9, closestCentroidCoordinate10 " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("running receive method");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start();
        try {
            inputHandler.send(new Object[]{6.4102, 3.4204, 9.1675, 9.0221, 0.5608, 7.1033, 7.7431, 4.6884, 6.8241, 8.0325});
            inputHandler.send(new Object[]{25.8, 23.5398, 20.07, 20.8239, 27.7256, 20.4674, 22.5265, 20.2619, 24.9773, 24.1583});
            inputHandler.send(new Object[]{47.36, 41.5787, 49.1591, 46.7029, 48.1067, 42.3356, 48.6527, 48.8769, 49.649, 41.847});
            inputHandler.send(new Object[]{3.8676, 5.1766, 3.1406, 2.2063, 3.1088, 9.8917, 6.8142, 5.8611, 3.3964, 2.2973});
            inputHandler.send(new Object[]{28.1475, 27.164, 24.0448, 26.2466, 27.1208, 26.3142, 29.336, 27.5347, 27.3927, 20.2745});
            inputHandler.send(new Object[]{43.9388, 45.3746, 47.8286, 48.1826, 44.9689, 47.6832, 41.5976, 45.9569, 48.2919, 45.3946});
            inputHandler.send(new Object[]{7.5591, 1.8557, 2.8265, 2.5882, 8.3309, 2.7693, 0.8656, 1.0833, 1.2891, 7.2464});
            inputHandler.send(new Object[]{20.0093, 24.9389, 26.5452, 28.9286, 29.9976, 20.1299, 20.2128, 25.1904, 29.9424, 27.8615});
            inputHandler.send(new Object[]{47.0938, 45.1418, 40.5231, 40.94, 41.7492, 47.9126, 44.0048, 47.7662, 43.004, 44.2468});
            inputHandler.send(new Object[]{6.8037, 2.1238, 4.8743, 7.4504, 3.009, 8.9197, 3.5681, 6.09, 7.1946, 7.5136});
            inputHandler.send(new Object[]{20.6254, 26.2655, 21.3912, 21.6066, 29.3236, 21.7912, 20.0862, 28.1948, 27.6278, 27.8911});
            inputHandler.send(new Object[]{43.5084, 46.9504, 48.2094, 41.2716, 43.0732, 43.7101, 46.6048, 49.7021, 42.698, 48.0274});
            inputHandler.send(new Object[]{4.7347, 7.9138, 5.0927, 6.137, 1.7307, 2.7228, 7.1644, 2.2177, 8.7546, 7.0981});
            inputHandler.send(new Object[]{23.3359, 20.4742, 20.0365, 21.4107, 24.5702, 28.6175, 27.2337, 22.3157, 23.2276, 23.0183});
            inputHandler.send(new Object[]{47.5688, 48.2002, 47.6935, 40.2836, 44.0251, 47.2869, 42.1774, 45.884, 46.9146, 43.8413});
            inputHandler.send(new Object[]{1.3836, 6.7456, 2.8098, 9.1107, 0.4434, 4.7182, 6.8168, 3.3619, 6.4666, 9.9873});
            inputHandler.send(new Object[]{23.0968, 23.8564, 21.3793, 27.3479, 26.7993, 27.7033, 24.5793, 20.708, 24.9513, 24.38});
            inputHandler.send(new Object[]{47.5254, 42.201, 42.3374, 46.2622, 49.5506, 43.6084, 40.8513, 41.2367, 48.598, 48.2386});
            inputHandler.send(new Object[]{2.4005, 1.2307, 8.6957, 4.9376, 4.7243, 9.806, 3.6459, 0.6252, 5.5007, 3.2297});
            inputHandler.send(new Object[]{24.276, 26.1401, 21.7168, 25.145, 27.4481, 29.1199, 24.3239, 21.3931, 23.1489, 23.3495});
            inputHandler.send(new Object[]{40.6311, 47.8634, 47.0588, 47.6989, 43.4766, 40.1323, 46.1682, 40.2228, 43.4037, 45.138});
            inputHandler.send(new Object[]{1.2974, 7.6531, 5.9637, 8.4445, 0.7973, 1.1196, 6.4436, 6.6067, 0.9517, 5.7066});
            inputHandler.send(new Object[]{27.1195, 21.7566, 24.3251, 24.8673, 24.2333, 29.2147, 25.6835, 20.8987, 25.8838, 27.8613});
            inputHandler.send(new Object[]{40.8754, 42.692, 44.6897, 48.9001, 46.9125, 41.2284, 43.0396, 43.4497, 46.2381, 44.258});
            inputHandler.send(new Object[]{5.238, 9.519, 3.9296, 0.0568, 2.1389, 5.3763, 3.0444, 8.3307, 9.9294, 6.105});
            inputHandler.send(new Object[]{27.0128, 29.1361, 27.5911, 29.3266, 27.2691, 28.9402, 21.5716, 26.8473, 28.6847, 27.7866});
            inputHandler.send(new Object[]{40.9929, 41.44, 43.8637, 44.7299, 45.2072, 45.2297, 42.7307, 42.8277, 40.1612, 45.8822});
            inputHandler.send(new Object[]{7.54, 3.6431, 4.3722, 0.94, 0.0468, 5.8606, 9.3025, 6.512, 4.1559, 6.4966});
            inputHandler.send(new Object[]{25.5837, 29.2094, 22.0906, 24.9178, 24.6216, 27.0414, 25.3236, 28.2608, 25.4164, 28.1341});
            inputHandler.send(new Object[]{40.882, 49.0899, 46.5846, 48.6681, 46.9042, 48.76, 42.222, 46.7812, 43.8035, 49.2306});
            inputHandler.send(new Object[]{6.4102, 3.4204, 9.1675, 9.0221, 0.5608, 7.1033, 7.7431, 4.6884, 6.8241, 8.0325});
            inputHandler.send(new Object[]{25.8, 23.5398, 20.07, 20.8239, 27.7256, 20.4674, 22.5265, 20.2619, 24.9773, 24.1583});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}