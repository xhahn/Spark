package com.xhahn.logs.capter1;

import com.xhahn.logs.ApacheAccessLog;
import com.xhahn.logs.Functions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * User: xhahn
 * Data: 2015/8/18/0018
 * Time: 9:28
 */
public class LogAnalyzer {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LogAnalyzer").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        if (args.length < 1) {
            System.err.println("Must specify an access log file!");
            System.exit(-1);
        }

        String log = args[0];
        JavaRDD<String> loglines = sc.textFile(log);

        // Convert the text log lines to ApacheAccessLog objects and cache them
        // since multiple transformations and actions will be called on that data.
        // items which are not match the accessable format are abandoned.
        JavaRDD<ApacheAccessLog> accessLogs =
                loglines.filter(Functions.FILTER_AccessLog).map(Functions.PARSE_LOG_LINE).cache();

        // Calculate statistics based on the content size.
        // Note how the contentSizes are cached as well since multiple actions
        // are called on that RDD.
        JavaRDD<Long> contentSizes =
                accessLogs.map(Functions.GET_CONTENT_SIZE).cache();
        System.out.println(String.format("Content Size Avg:%s,Min:%s,Max:%s"
                , contentSizes.reduce(Functions.SUM_REDUCER) / contentSizes.count()
                , contentSizes.min(Functions.LONG_NATURAL_ORDER_COMPARATOR)
                , contentSizes.max(Functions.LONG_NATURAL_ORDER_COMPARATOR)));

        // Compute Response Code to Count.
        List<Tuple2<Integer, Long>> responseCodeToCount = accessLogs
                .mapToPair(Functions.GET_RESPONSE_CODE)
                .reduceByKey(Functions.SUM_REDUCER)
                .take(50);
        System.out.println(String.format("Response code  counts:%s", responseCodeToCount));


        List<String> ipAddresses = accessLogs
                .mapToPair(Functions.GET_IP_ADDRESS)
                .reduceByKey(Functions.SUM_REDUCER)
                .filter(Functions.FILTER_GREATER_10)
                .map(Functions.GET_TUPLE_FIRST)
                .take(50);
        System.out.println(String.format("ipAddresses > 10:%s", ipAddresses));

        //compute the top endpoints accessed on this server
        //according to how many times the endpoint was accessed.
        List<Tuple2<String, Long>> topEndpoints = accessLogs
                .mapToPair(Functions.GET_ENDPOINT)
                .reduceByKey(Functions.SUM_REDUCER)
                .top(10, new Functions.ValueComparator<String, Long>(
                        Functions.LONG_NATURAL_ORDER_COMPARATOR
                ));
        System.out.println(String.format("top Endpoints:%s", topEndpoints));


        sc.stop();
    }
}
