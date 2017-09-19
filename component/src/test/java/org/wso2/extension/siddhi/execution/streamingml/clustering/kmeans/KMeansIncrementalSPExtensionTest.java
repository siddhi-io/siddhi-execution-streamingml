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

package org.wso2.extension.siddhi.execution.streamingml.clustering.kmeans;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class KMeansIncrementalSPExtensionTest {

    private static final Logger logger = Logger.getLogger(KMeansIncrementalSPExtensionTest.class);
    private volatile AtomicInteger count;
    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
    }

    @Test
    public void testClusteringLengthWindow2D_0() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case for 2D data points with decay rate");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model1', 0.2, 2, x, y) " +
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

    @Test
    public void testClusteringLengthWindow2D_1() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case for 2D data points without decay rate");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model1', 2, x, y) " +
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


    @Test
    public void testClusteringLengthWindow2D_2() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case for 2D data points debugging");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model1', 0.2, 2, x, y) " +
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
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testClusteringLengthWindow2D_3() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case for 2D data points of type other than double");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model1', 0.2, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            inputHandler.send(new Object[]{5, 7});
            inputHandler.send(new Object[]{27.458f, 23.8848f});
            inputHandler.send(new Object[]{3.078, 9.1072});
            inputHandler.send(new Object[]{8, 26.7484});
            inputHandler.send(new Object[]{2.2602, 4.6408});
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClusteringLengthWindow2D_4() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate modelName to be constant");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, modelName String);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental(modelName, 0.2, 2, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("modelName has to be a constant but found " +
                    "org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_5() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate decayRate to be constant");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, decayRate double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model5', decayRate, 2, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("decayRate has to be a constant but found " +
                    "org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_6() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate numberOfClusters to be constant");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double, numberOfClusters int);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental" +
                        "('model6', 0.2, numberOfClusters, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("numberOfClusters has to be a constant but " +
                    "found org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_7() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate attribute_0 to be variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model7', 0.2, 2, 5.0, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("4th parameter is not an attribute " +
                    "(VariableExpressionExecutor) present in the stream definition. Found a " +
                    "org.wso2.siddhi.core.executor.ConstantExpressionExecutor"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_8() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate attribute_1 to be variable");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model8', 0.2, 2, x, 4.0) " +
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
                    "org.wso2.siddhi.core.executor.ConstantExpressionExecutor"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_9() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate modelName to be String");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental(5L, 0.2, 2, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("modelName should be of type String but " +
                    "found LONG"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_10() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate decayRate to be double");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model10', 1L, 2, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("The second query parameter should either be " +
                    "decayRate " +
                    "or numberOfClusters which should be of type double or int respectively but found LONG"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_11() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate decayRate is in [0,1] when it is " +
                "larger than 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model11', 1.4, 2, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("decayRate should be in [0,1] but given as 1.4"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_12() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate decayRate is in [0,1] when it is " +
                "smaller than 0");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model12', -0.3, 2, x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("decayRate should be in [0,1] " +
                    "but given as -0.3"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_13() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case to validate numberOfClusters is int");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model12', 0.3, 'hi', x, y) " +
                        "select euclideanDistanceToClosestCentroid, closestCentroidCoordinate1, " +
                        "closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        } catch (Exception e) {
            logger.info("Error caught");
            AssertJUnit.assertTrue(e instanceof SiddhiAppCreationException);
            AssertJUnit.assertTrue(e.getCause().getMessage().contains("numberOfClusters should be of type int " +
                    "but found STRING"));
        }
    }

    @Test
    public void testClusteringLengthWindow2D_14() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case for duplicate seed rejection");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model1', 0.2, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            inputHandler.send(new Object[]{5.7905, 7.7499});
            inputHandler.send(new Object[]{5.7905, 7.7499});
            inputHandler.send(new Object[]{27.458, 23.8848});
            inputHandler.send(new Object[]{3.078, 9.1072});
            inputHandler.send(new Object[]{28.326, 26.7484});
            inputHandler.send(new Object[]{2.2602, 4.6408});
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClusteringLengthWindow2D_15() throws Exception {
        //compare the resulting centroid list after all the events are processed against the results of testCase_0
        //data are same except the second data point which is very close to the first one. the first two data points
        // which are very close to each other are taken as initial centroids. but as the stream progresses the model
        //adjusts itself and the two centroids drift apart
        logger.info("KMeansIncrementalSPExtension Test - Test case for initial seeds that are very close");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model1', 0.2, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            inputHandler.send(new Object[]{5.7905, 7.7499});
            inputHandler.send(new Object[]{5.7907, 7.7501});
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

    @Test
    public void testClusteringLengthWindow2D_16() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case for reusing the model in a different query");
        //compare final centroid list against the final result from testCase_0. same data
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream1 = "@App:name('KMeansIncrementalTestApp16') \n" +
                "define stream InputStream1 (x double, y double);";
        String inputStream2 = "@App:name('KMeansIncrementalTestApp16') \n" +
                "define stream InputStream2 (x double, y double);";

        String query1 = (
                "@info(name = 'query1') " +
                        "from InputStream1#streamingml:kMeansIncremental('model16', 0.2, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        String query2 = (
                "@info(name = 'query2') " +
                        "from InputStream2#streamingml:kMeansIncremental('model16', 0.2, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream1 + query1);

        siddhiAppRuntime.start();
        InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("InputStream1");
        try {
            inputHandler1.send(new Object[]{5.7905, 7.7499});
            inputHandler1.send(new Object[]{27.458, 23.8848});
            inputHandler1.send(new Object[]{3.078, 9.1072});
            inputHandler1.send(new Object[]{28.326, 26.7484});
            inputHandler1.send(new Object[]{2.2602, 4.6408});
            inputHandler1.send(new Object[]{27.3099, 26.1816});
            inputHandler1.send(new Object[]{0.9441, 0.6502});
            inputHandler1.send(new Object[]{23.9204, 27.6745});
            inputHandler1.send(new Object[]{2.0499, 9.9546});

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream2 + query2);
            InputHandler inputHandler2 = siddhiAppRuntime.getInputHandler("InputStream2");

            inputHandler2.send(new Object[]{23.7947, 20.8627});
            inputHandler2.send(new Object[]{5.8456, 6.8879});
            inputHandler2.send(new Object[]{26.7315, 25.5368});
            inputHandler2.send(new Object[]{5.8812, 5.9116});
            inputHandler2.send(new Object[]{24.5343, 26.77});
            inputHandler2.send(new Object[]{4.3866, 0.3132});
            inputHandler2.send(new Object[]{22.7654, 25.1381});
            inputHandler2.send(new Object[]{7.7824, 9.2299});
            inputHandler2.send(new Object[]{23.5167, 24.1244});
            inputHandler2.send(new Object[]{5.3086, 9.7503});
            inputHandler2.send(new Object[]{25.47, 25.8574});
            inputHandler2.send(new Object[]{20.2568, 28.7882});
            inputHandler2.send(new Object[]{2.9951, 3.9887});
        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testClusteringLengthWindow2D_17() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case for restoring from restart");
        //compare final centroid list against the final result from testCase_0. same data
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(new InMemoryPersistenceStore());
        String inputStream = "@App:name('KMeansIncrementalTestApp17') \n" +
                "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model17', 0.2, 2, x, y) " +
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
            inputHandler.send(new Object[]{23.9204, 27.6745});
            inputHandler.send(new Object[]{2.0499, 9.9546});

            siddhiManager.persist();
            Thread.sleep(1000);
            siddhiAppRuntime.shutdown();
            Thread.sleep(1000);

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
            siddhiAppRuntime.start();
            siddhiManager.restoreLastState();
            inputHandler = siddhiAppRuntime.getInputHandler("InputStream");

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

    @Test
    public void testClusteringLengthWindow2D_18() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - Test case for sending data from file");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x double, y double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model1', 0.2, 2, x, y) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, x, y " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        Scanner scanner = null;
        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            File file = new File("src/test/resources/kMeansFileTest.csv");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            scanner = new Scanner(bufferedReader);

            while (scanner.hasNext()) {
                String eventStr = scanner.nextLine();
                String[] event = eventStr.split(",");
                inputHandler.send(new Object[]{Double.valueOf(event[0]), Double.valueOf(event[1])});
            }

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    /*@Test
    public void testClusteringLengthWindow2D_19() throws Exception {
        logger.info("KMeansIncrementalSPExtension Test - standard dataset at " +
                "https://archive.ics.uci.edu/ml/datasets/3D+Road+Network+%28North+Jutland%2C+Denmark%29");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (x1 double, x2 double, x3 double, x4 double);";

        String query = (
                "@info(name = 'query1') " +
                        "from InputStream#streamingml:kMeansIncremental('model19', 0.001, 2, x1, x2, x3, x4) " +
                        "select closestCentroidCoordinate1, closestCentroidCoordinate2, " +
                        "closestCentroidCoordinate3, closestCentroidCoordinate4 " +
                        "insert into OutputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);

        Scanner scanner = null;
        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");
        try {
            File file = new File("src/test/resources/3D_spatial_network.csv");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            scanner = new Scanner(bufferedReader);

            while (scanner.hasNext()) {
                String eventStr = scanner.nextLine();
                String[] event = eventStr.split(",");
                inputHandler.send(new Object[]{Double.valueOf(event[0]), Double.valueOf(event[1]),
                        Double.valueOf(event[2]), Double.valueOf(event[3])});
            }

        } catch (Exception e) {
            logger.error(e.getCause().getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }*/

}
