package com.xhahn.gmm;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;


/**
 * User: xhahn
 * Data: 2015/8/26/0026
 * Time: 10:13
 */
public class SummaryStatistics {
    private JavaRDD<Vector> mat;

    public SummaryStatistics(JavaRDD<Vector> mat) {
        this.mat = mat;
    }

    public void Summary() {
        MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
        System.out.println("平均： " + summary.mean()); // a dense vector containing the mean value for each column
        System.out.println("方差： " + summary.variance()); // column-wise variance
        System.out.println("总数： " + summary.numNonzeros()); // number of nonzeros in each column
    }
}
