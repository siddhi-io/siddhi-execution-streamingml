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

package io.siddhi.extension.execution.streamingml.clustering.kmeans;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class KMeansMiniBatchSPExtensionTest {
    private static final Logger logger = LogManager.getLogger(KMeansMiniBatchSPExtensionTest.class);
    private volatile AtomicInteger count;

    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }

    @Test
    public void testClusteringLengthWindow2D_0() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case for 2D data points with decay rate");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.2, 10, 20, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                for (Event event: inEvents) {
                    count.incrementAndGet();
                    switch (count.get()) {
                        case 21:
                            AssertJUnit.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{
                                    event.getData(0), event.getData(1)});
                            break;
                        case 22:
                            AssertJUnit.assertArrayEquals(new Double[]{4.3327, 6.4196}, new Object[]{
                                    event.getData(0), event.getData(1)});
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
            SiddhiTestHelper.waitForEvents(200, 3, count, 5000);
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClusteringLengthWindow2D_1() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case for 2D data points without hyper params");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                for (Event event: inEvents) {
                    count.incrementAndGet();

                    switch (count.get()) {
                        case 21:
                            AssertJUnit.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{
                                    event.getData(0), event.getData(1)});
                            break;
                        case 22:
                            AssertJUnit.assertArrayEquals(new Double[]{4.3327, 6.4196}, new Object[]{
                                    event.getData(0), event.getData(1)});
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

            SiddhiTestHelper.waitForEvents(200, 3, count, 5000);
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClusteringLengthWindow3D5k() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case for 3D data points with k=5. demonstrating " +
                "seperate thread retraining");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, z double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(5, 0.1, 10, 20, x, y, z) " +
                        "select x, y, z, closestCentroidCoordinate1, closestCentroidCoordinate2, " +
                        "closestCentroidCoordinate3 " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                EventPrinter.print(events);
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
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClusteringLengthWindow2D_2() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case for sending euclidean distance as output");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.2, 10, 20, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                EventPrinter.print(events);
                for (Event event: events) {
                    count.incrementAndGet();

                    switch (count.get()) {
                        case 21:
                            AssertJUnit.assertEquals(6.2127, event.getData(0));
                            break;
                        case 22:
                            AssertJUnit.assertEquals(2.7746, event.getData(0));
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
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = {SiddhiAppCreationException.class})
    public void testClusteringLengthWindow2D_5() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate numberOfClusters to be constant");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, numberOfClusters int);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch" +
                        "(numberOfClusters, 0.2, 10, 20, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
    }

    @Test
    public void testClusteringLengthWindow2D_6() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate maximumIterations to be constant");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, maximumIterations int);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.2, maximumIterations, 20, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Maximum iterations has to be a constant " +
                    "but found " +
                    "io.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_7() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate numberOfEventsToRetrain to be constant");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, numberOfEventsToRetrain int);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch" +
                        "(2, 0.2, 10, numberOfEventsToRetrain, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("numberOfEventsToRetrain has to be a " +
                    "constant but found " +
                    "io.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_8() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate attribute_0 to be variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.2, 10, 20, 5.0, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("5th parameter is not an attribute " +
                    "(VariableExpressionExecutor) present in the stream definition. Found a " +
                    "io.siddhi.core.executor.ConstantExpressionExecutor"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_9() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate attribute_1 to be variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.2, 10, 20, x, 5.0) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("6th parameter is not an attribute " +
                    "(VariableExpressionExecutor) present in the stream definition. Found a " +
                    "io.siddhi.core.executor.ConstantExpressionExecutor"));
        }
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClusteringLengthWindow2D_11() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate decayRate to be double");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 'hi', 10, 20, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
    }

    @Test
    public void testClusteringLengthWindow2D_12() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate decayRate is in [0,1] when it is " +
                "larger than 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 1.4, 10, 20, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Decay rate should be in [0,1] but " +
                    "given as 1.4"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_13() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate decayRate is in [0,1] when it is " +
                "smaller than 0");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, -0.3, 10, 20, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("Decay rate should be in [0,1] but " +
                    "given as -0.3"));
        }
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClusteringLengthWindow2D_14() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate numberOfClusters is int");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2.1, 0.3, 10, 20, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClusteringLengthWindow2D_15() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate maximumIterations is int");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.3, 'aa', 20, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
    }

    @Test (expectedExceptions = {SiddhiAppCreationException.class})
    public void testClusteringLengthWindow2D_16() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case to validate numberOfEventsToRetrain is int");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.3, 10, 1L, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
    }

    @Test
    public void testClusteringLengthWindow2D_18() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case for restoring from restart");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());
        String inputStream = "@App:name('KMeansTestApp') \n" +
                "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.2, 10, 20, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                for (Event event: inEvents) {
                    count.incrementAndGet();

                    switch (count.get()) {
                        case 21:
                            AssertJUnit.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{
                                    event.getData(0), event.getData(1)});
                            break;
                        case 22:
                            AssertJUnit.assertArrayEquals(new Double[]{4.3327, 6.4196}, new Object[]{
                                    event.getData(0), event.getData(1)});
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

            siddhiManager.persist();
            Thread.sleep(500);
            siddhiAppRuntime.shutdown();
            Thread.sleep(1000);

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(inEvents);
                    for (Event event: inEvents) {
                        count.incrementAndGet();

                        switch (count.get()) {
                            case 23:
                                AssertJUnit.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{
                                        event.getData(0), event.getData(1)});
                                break;
                            case 24:
                                AssertJUnit.assertArrayEquals(new Double[]{25.3827, 25.2779}, new Object[]{
                                        event.getData(0), event.getData(1)});
                                break;
                            case 25:
                                AssertJUnit.assertArrayEquals(new Double[]{4.3327, 6.4196}, new Object[]{
                                        event.getData(0), event.getData(1)});
                                break;

                        }
                    }
                }
            });
            siddhiAppRuntime.start();
            siddhiManager.restoreLastState();
            Thread.sleep(500);
            inputHandler = siddhiAppRuntime.getInputHandler("InputStream");

            inputHandler.send(new Object[]{25.47, 25.8574});
            inputHandler.send(new Object[]{20.2568, 28.7882});
            inputHandler.send(new Object[]{2.9951, 3.9887});

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClusteringLengthWindow2D_19() throws Exception {
        logger.info("KMeansMiniBatchSPExtension Test - Test case for restoring from restart before initial training");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());
        String inputStream = "@App:name('KMeansTestApp') \n" +
                "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansMiniBatch(2, 0.2, 10, 20, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

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

            siddhiManager.persist();
            Thread.sleep(500);

            siddhiAppRuntime.shutdown();
            Thread.sleep(500);

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(inEvents);
                    for (Event event: inEvents) {
                        count.incrementAndGet();

                        switch (count.get()) {
                            case 20:
                                AssertJUnit.assertArrayEquals(new Double[]{25.3827, 25.2779},
                                        new Object[]{event.getData(0),
                                        event.getData(1)});
                                break;
                            case 21:
                                AssertJUnit.assertArrayEquals(new Double[]{25.3827, 25.2779},
                                        new Object[]{event.getData(0),
                                        event.getData(1)});
                                break;
                            case 22:
                                AssertJUnit.assertArrayEquals(new Double[]{4.3327, 6.4196},
                                        new Object[]{event.getData(0),
                                        event.getData(1)});
                                break;
                        }
                    }
                }
            });
            siddhiAppRuntime.start();
            siddhiManager.restoreLastState();
            inputHandler = siddhiAppRuntime.getInputHandler("InputStream");

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

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

}

