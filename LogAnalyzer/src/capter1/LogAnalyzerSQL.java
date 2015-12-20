package com.xhahn.logs.capter1;

import com.xhahn.logs.ApacheAccessLog;
import com.xhahn.logs.Functions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.List;

/**
 * User: xhahn
 * Data: 2015/8/18/0018
 * Time: 16:10
 */
public class LogAnalyzerSQL {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LogAnalyzerSQL").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        if (args.length < 1) {
            System.out.println("Must specify an access logs file!");
            System.exit(-1);
        }

        String logfile = args[0];
        JavaRDD<ApacheAccessLog> accessLog = sc
                .textFile(logfile).filter(Functions.FILTER_AccessLog).map(Functions.PARSE_LOG_LINE).cache();

        DataFrame sqlDataFrame = sqlContext.createDataFrame(accessLog, ApacheAccessLog.class);
        sqlDataFrame.registerTempTable("logs");
        sqlContext.cacheTable("logs");

        // Calculate statistics based on the content size.
        Row contentSizeState = sqlContext
                .sql("SELECT SUM(contentSize),COUNT(*),MIN(contentSize),MAX(contentSize) FROM logs")
                .javaRDD()
                .collect()
                .get(0);
        System.out.println(String.format("ContentSize AVG:%s,MIN:%s,MAX:%s"
                , contentSizeState.getLong(0) / contentSizeState.getLong(1)
                , contentSizeState.getLong(2)
                , contentSizeState.getLong(3)
        ));

        // Compute Response Code to Count.
        // Note the use of "LIMIT 1000" since the number of responseCodes
        // can potentially be too large to fit in memory.
        List<Tuple2<Integer, Long>> responseCodeToCount = sqlContext.
                sql("SELECT responseCode,COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
                .javaRDD()
                .mapToPair(Functions.GET_sqlRESPONSE_CODE)
                .collect();
        System.out.println(
                String.format("Response code counts: %s", responseCodeToCount));

        // Any IPAddress that has accessed the server more than 10 times.
        List<String> ipAddress = sqlContext
                .sql("SELECT ipAddress,COUNT(*)AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
                .javaRDD()
                .map(new Function<Row, String>() {
                    @Override
                    public String call(Row row) throws Exception {
                        return row.getString(0);
                    }
                })
                .collect();
        System.out.println(
                String.format("IPAddress > 10 :%s", ipAddress));

        // Top Endpoints.
        List<Tuple2<String, Long>> endpoint = sqlContext
                .sql("SELECT endpoint,COUNT(*)AS total FROM lo" +
                        "gs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .javaRDD()
                .map(new Function<Row, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> call(Row row) throws Exception {
                        return new Tuple2<String, Long>(row.getString(0), row.getLong(1));
                    }
                })
                .collect();

        System.out.println(
                String.format("Top Endpoints 10 :%s", endpoint));

        sc.stop();

    }
}
