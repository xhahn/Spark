package com.xhahn.MainAnalyzer;

import com.xhahn.LogAnalyzer.ApacheLogAnalyzer;
import com.xhahn.LogAnalyzer.IPSLogAnalyzer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * User: xhahn
 * Data: 2016/12/27/0027
 * Time: 17:20
 */
public class LogAnalyzerMain {

    //apache日志处理相关地址常量
    static final String APACHELOGPATH = "src\\main\\resources\\apache_onemonth.txt";
    static final String APACHELOGPATH_OUT_CONTENTCLEAN = "src\\main\\out\\apache_onemonth_out_contentClearn";
    static final String APACHELOGPATH_CLEARNED = "src\\main\\resources\\contentClearn";
    static final String APACHELOGPATH_CLEARNED_HDFS = "xhahn/data/hustlog/apache/input/contentClearn";

    //ips日志处理相关地址常量
    static final String IPSLOGPATH = "src\\main\\resources\\ips_20161201_halfday.txt";
    static final String IPSLOGPATH_OUT_FORMATCLEAN = "src\\main\\out\\IPS_20161201_halfday_out_formatClean";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("logAnalyzer").setMaster("local");
        JavaSparkContext spark = new JavaSparkContext(conf);


        System.setProperty("hadoop.home.dir", "E:\\xhahn\\hadoop\\hadoop-2.7.3");

        runApacheLogAnalyzer(spark);
        runIPSLogAnalyzer(spark);

        spark.stop();
    }

    private static void runApacheLogAnalyzer(JavaSparkContext spark) {

        ApacheLogAnalyzer apacheLogAnalyzer = ApacheLogAnalyzer.getApacheLogAnalyzer();
        apacheLogAnalyzer.runApacheLogFormatAndContentClearner(spark, APACHELOGPATH, APACHELOGPATH_OUT_CONTENTCLEAN);
        //apacheLogAnalyzer.runApacheLogMissingValueClearner(spark, APACHELOGPATH_CLEARNED_HDFS);
        apacheLogAnalyzer.runApacheLogMissingValueClearner(spark, APACHELOGPATH_CLEARNED);

    }

    private static void runIPSLogAnalyzer(JavaSparkContext spark) {

        IPSLogAnalyzer ipsLogAnalyzer = IPSLogAnalyzer.getIPSLogAnalyzer();
        ipsLogAnalyzer.runIPSLogFormatAndContentClearner(spark, IPSLOGPATH, IPSLOGPATH_OUT_FORMATCLEAN);
    }

}
