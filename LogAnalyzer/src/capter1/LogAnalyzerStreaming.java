package com.xhahn.logs.capter1;

import com.xhahn.logs.ApacheAccessLog;
import com.xhahn.logs.Functions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * User: xhahn
 * Data: 2015/8/18/0018
 * Time: 17:43
 *
 *  * To feed the new lines of some logfile into a socket, run this command:
 *   % tail -f [[YOUR_LOG_FILE]] | nc -lk 9999
 *
 */
public class LogAnalyzerStreaming {
    // Stats will be computed for the last window length of time.
    private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
    // Stats will be computed every slide interval time.
    private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LogAnalyzerStreaming").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // This sets the update window to be every 10 seconds.-
        JavaStreamingContext jssc = new JavaStreamingContext(sc, SLIDE_INTERVAL);

        JavaReceiverInputDStream<String> logDataDStream =
                jssc.socketTextStream("local", 9999);

        // A DStream of Apache Access Logs.
        JavaDStream<ApacheAccessLog> accessLogDStream =
                logDataDStream.filter(Functions.FILTER_AccessLog).map(Functions.PARSE_LOG_LINE).cache();

        // Splits the accessLogDStream into a dstream of time windowed rdd's of apache access logs.
        JavaDStream<ApacheAccessLog> windowDStream =
                accessLogDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL);

        windowDStream.foreachRDD(
                new Function<JavaRDD<ApacheAccessLog>, Void>() {
                    @Override
                    public Void call(JavaRDD<ApacheAccessLog> accessLogs) throws Exception {
                        if (accessLogs.count() == 0) {
                            System.out.println("no access logs in this interval.");
                            return null;
                        }

                        // *** Note that this is code copied verbatim from LogAnalyzer.java.

                        // Calculate statistics based on the content size.
                        JavaRDD<Long> contentSizes =
                                accessLogs.map(Functions.GET_CONTENT_SIZE).cache();
                        System.out.println(String.format("Content Size Avg:%s,Min:%s,Max:%s"
                                , contentSizes.reduce(Functions.SUM_REDUCER) / contentSizes.count()
                                , contentSizes.min(Functions.LONG_NATURAL_ORDER_COMPARATOR)
                                , contentSizes.max(Functions.LONG_NATURAL_ORDER_COMPARATOR)));

                        // Compute Response Code to Count.
                        List<Tuple2<Integer, Long>> responseCodeToCount =
                                accessLogs.mapToPair(Functions.GET_RESPONSE_CODE)
                                        .reduceByKey(Functions.SUM_REDUCER)
                                        .take(100);
                        System.out.println("Response code counts: " + responseCodeToCount);

                        // Any IPAddress that has accessed the server more than 10 times.
                        List<String> ipAddresses = accessLogs
                                .mapToPair(Functions.GET_IP_ADDRESS)
                                .reduceByKey(Functions.SUM_REDUCER)
                                .filter(Functions.FILTER_GREATER_10)
                                .map(Functions.GET_TUPLE_FIRST)
                                .take(100);
                        System.out.println("IPAddresses > 10 times: " + ipAddresses);

                        // Top Endpoints.
                        List<Tuple2<String, Long>> topEndpoints = accessLogs
                                .mapToPair(Functions.GET_ENDPOINT)
                                .reduceByKey(Functions.SUM_REDUCER)
                                .top(10, new Functions.ValueComparator<String, Long>(
                                        Functions.LONG_NATURAL_ORDER_COMPARATOR));
                        System.out.println("Top Endpoints: " + topEndpoints);

                        return null;
                    }
                });
        jssc.start();
        jssc.awaitTermination();
    }
}
