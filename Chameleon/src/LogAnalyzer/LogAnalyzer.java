package com.xhahn.gmm.LogAnalyzer;

import com.xhahn.gmm.Chameleon.ChameleonTool;
import com.xhahn.gmm.Functions;
import com.xhahn.gmm.SummaryStatistics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;

import java.util.*;


/**
 * User: xhahn
 * Data: 2015/8/26/0026
 * Time: 10:24
 */
public class LogAnalyzer {
    private static final String savaPath = "F:\\myGMMModel";
    private static final String tempFilePath = "F:\\acm\\idea\\temp.txt";
    private static final String outFile = "F:\\acm\\idea\\xhahn\\GMMLogAnalyzer\\out";
    private static final int numOfCluster = 4;
    public static Map<String, int[]> studentsTime = new HashMap<>();
    public static double[][] outPut;
    public static int count=0;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("LogAnalyzer").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        if (args.length < 1) {
            System.err.println("Must set a log file");
            System.exit(-1);
        }

        String logfile = args[0];
        JavaRDD<String> file = sc.textFile(logfile);

        //得到合乎要求的日志文件
        JavaRDD<AccessLogFile> LogFile = file.filter(Functions.FILTER_ACCESSLOGFILE).map(Functions.PARSE_ACCESSLOGFILE).cache();
        JavaRDD<Vector> GMMFile = LogFile.map(Functions.GET_ONLINE_SEC).map(Functions.PARSE_VECTOR);
        JavaRDD<Vector> GMMFile2 = LogFile.map(Functions.GET_ONLINE_SEC).filter(Functions.FILTER_ONLINE_SEC_LESS10).map(Functions.PARSE_VECTOR);

        //统计分析
        SummaryStatistics summaryStatistics = new SummaryStatistics(GMMFile);
        SummaryStatistics summaryStatistics2 = new SummaryStatistics(GMMFile2);
        summaryStatistics.Summary();//总体的统计信息
        System.out.println("以上为总体连接时长统计信息");
        summaryStatistics2.Summary();//连接时间少于10s的统计信息
        System.out.println("以上为连接时长少于10s部分统计信息");

        //计算每条记录在各个时间段内的连接时长
        LogFile.filter(Functions.FILTER_ONLINE_SEC_OVER10).foreach(Functions.ACCUMULATE_TIME);

        //将结果输出到文件中保存
        Set<Map.Entry<String, int[]>> entrySet = studentsTime.entrySet();
        Functions.storeFile(entrySet, tempFilePath);

        /**
         * Chameleon
         * @param filePath : 待处理文件路径
         * @param k : k-近邻的k设置
         * @param minMetric : 度量函数阈值
         */
        String filePath = tempFilePath;
        int k = 1;
        double minMetric = 0.01;
        ChameleonTool tool = new ChameleonTool(filePath, k, minMetric);
        int[] result = tool.buildCluster();
        System.out.println(result[0]+" "+result[1]);

        while (result[0]-1>=0) {
            count = 0;
            outPut = new double[result[1]][12];
            sc.textFile(outFile + "//out"+(result[0]-1)+".txt").map(Functions.PARSE_ACCESSOUTFILE).foreach(Functions.ACCUMULATE_OUTPUT);
            List<double[]> arr = Arrays.asList(outPut);

            JavaRDD<Vector> output = sc.parallelize(arr).map(Functions.PARSE_VECTOR2);
            SummaryStatistics resultSummaryStatistics = new SummaryStatistics(output);
            resultSummaryStatistics.Summary();
            System.out.println("以上为第"+(result[0]-1)+"个簇的统计信息");

            result[0]--;
        }
        
        }
    }
