package org.wso2.extension.siddhi.execution.ml.kmeansFromScratchTest;

import javafx.scene.chart.PieChart;
import org.wso2.extension.siddhi.execution.ml.kmeansFromScratch.Clusterer;
import org.wso2.extension.siddhi.execution.ml.kmeansFromScratch.Coordinates;
import org.wso2.extension.siddhi.execution.ml.kmeansFromScratch.DataPoint;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by niruhan on 7/11/17.
 */
public class ClustererTest {

    private static int dimensionality = 2;
    private static ArrayList<DataPoint> dataPointsArray = new ArrayList<>();
    private static ArrayList<DataPoint> a = new ArrayList<>();
    private static double[][] data = new double[][]{{6.0,1.0},{1.0,5.0}, {5.0,2.0},
            {5.0,3.0},{6.0,3.0},{7.0,2.0},{2.0,4.0},{1.0,6.0},{2.0,6.0},{3.0,5.0},{3.0,6.0}};

    public static void main(String[] args) {
        Clusterer myClusterer = new Clusterer(2, 10);



        for (int i=0; i<11; i++) {
            DataPoint p = new DataPoint(dimensionality);
            p.setCoordinates(data[i]);
            dataPointsArray.add(p);
        }

        myClusterer.initialize(dataPointsArray);
        myClusterer.assignToCluster(dataPointsArray);

        /*for (DataPoint a: dataPointsArray) {
            System.out.print(Arrays.toString(a.getCoordinates()));
            System.out.print(" - ");
            System.out.println(Arrays.toString(a.getAssociatedCentroid().getCoordinates()));
        }*/
        /*int k=2;
        ArrayList<double[]> total = new ArrayList<>();
        int[] count = new int[k];
        for (int i=0; i<k; i++) {
            count[i] = 0;
            total.add(new double[dimensionality]);
        }
        System.out.println(total.get(0));
        System.out.println(total.get(1));*/
        /*DataPoint p = new DataPoint(dimensionality);
        double[] x = new double[]{3,6};
        p.setCoordinates(x);
        int ind = dataPointsArray.indexOf(p);
        System.out.println(ind);*/
        double[] res = new double[2];
        Arrays.setAll(res, i -> data[3][i] + data[1][i]);
        System.out.println(Arrays.toString(res));
    }



}
