package com.xhahn.LogAnalyzer;

import com.xhahn.Clearner.ApacheClearner;
import com.xhahn.JavaBean.ApacheLog;
import com.xhahn.LogParser.ApacheParser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * User: xhahn
 * Data: 2017/1/18/0018
 * Time: 17:28
 */
public class ApacheLogAnalyzer implements Serializable {

    private static volatile ApacheLogAnalyzer instance;

    private ApacheLogAnalyzer() {
    }

    public static ApacheLogAnalyzer getApacheLogAnalyzer() {
        if (instance == null) {
            synchronized (ApacheLogAnalyzer.class) {
                if (instance == null)
                    instance = new ApacheLogAnalyzer();
            }
        }

        return instance;
    }

    public void runApacheLogFormatAndContentClearner(JavaSparkContext spark, String sourcePath, String outputPath) {
        //过滤格式残缺的日志
        //时间格式错误，日志残缺
        JavaRDD<String> apacheLog_format = spark.textFile(sourcePath)
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String log) throws Exception {
                        ApacheClearner apacheCleaner = ApacheClearner.getApacheClearner();
                        return apacheCleaner.isCompleted(log);
                    }
                }).cache();


        JavaRDD<ApacheLog> apacheLogRDD = apacheLog_format.map(new Function<String, ApacheLog>() {
            @Override
            public ApacheLog call(String log) throws Exception {
                ApacheParser apacheParser = ApacheParser.getApacheParser();
                ApacheLog apacheLog = apacheParser.parse(log);
                return apacheLog;
            }
        }).cache();

        //过滤内容不符要求的日志
        JavaRDD<ApacheLog> apacheLogClearned = apacheLogRDD.filter(new Function<ApacheLog, Boolean>() {
            @Override
            public Boolean call(ApacheLog apacheLog) throws Exception {
                ApacheClearner apacheClearner = ApacheClearner.getApacheClearner();
                return apacheClearner.isValid(apacheLog);
            }
        }).cache();

        System.out.println(apacheLogClearned.map(new Function<ApacheLog, String>() {
            @Override
            public String call(ApacheLog apacheLog) throws Exception {
                return apacheLog.getIp();
            }
        }).distinct().count());

        //保存为纯文本，字段之间以"<=>"分隔
        apacheLogClearned.repartition(1).map(new Function<ApacheLog, String>() {
            @Override
            public String call(ApacheLog apacheLog) throws Exception {
                return apacheLog.getIp()+"<=>"+apacheLog.getDate()+"<=>"+apacheLog.getHttpMethod()+"<=>"+apacheLog.getUrl()+"<=>"+apacheLog.getHttpProtocol()+"<=>"+apacheLog.getStatue()+"<=>"+apacheLog.getData();
            }
        }).saveAsTextFile(outputPath);
    }

    public void runApacheLogMissingValueClearner(JavaSparkContext jSpark,String sourcePath) {

        SQLContext spark = new SQLContext(jSpark);
        JavaRDD<ApacheLog> apacheLogRDD = jSpark.textFile(sourcePath)
                .map(new Function<String, ApacheLog>() {
                    @Override
                    public ApacheLog call(String s) throws Exception {
                        ApacheParser apacheParser = ApacheParser.getApacheParser();
                        return apacheParser.parseClearnLog(s);
                    }
                });

        //计算X和data互信息
        JavaPairRDD<Integer, String> apacheLog_data_X = apacheLogRDD.filter(new Function<ApacheLog, Boolean>() {
            @Override
            public Boolean call(ApacheLog apacheLog) throws Exception {
                if (apacheLog.getData() == -1)
                    return false;
                return true;
            }
        }).mapToPair(new PairFunction<ApacheLog, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(ApacheLog apacheLog) throws Exception {
                return new Tuple2<Integer, String>(apacheLog.getData(), apacheLog.getDate() + "");
            }
        }).sample(false, 0.001);

        //p(Y)
        Map<Integer, Double> apache_p_date = apacheLog_data_X.keys().mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return new Tuple2<Integer, Double>(integerIntegerTuple2._1(), 1.0 * integerIntegerTuple2._2() / 2064);
            }
        }).collectAsMap();

        //p(X)
        Map<String, Double> apache_X_date = apacheLog_data_X.values().mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Integer> integerIntegerTuple2) throws Exception {
                return new Tuple2<String, Double>(integerIntegerTuple2._1(), 1.0 * integerIntegerTuple2._2() / 2064);
            }
        }).collectAsMap();

        //各类中x总数
        Map<Integer, Integer> data_n = apacheLog_data_X.mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<Integer, Integer>(integerStringTuple2._1(), 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).collectAsMap();

        //各类中各特征值数
        Map<Integer, Map<String, Integer>> numberOfXindata = apacheLog_data_X.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<String, Integer>(integerStringTuple2._1() + "<=>" + integerStringTuple2._2(), 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<Integer, Tuple2<String, Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, Tuple2<String, Integer>>(Integer.parseInt(stringIntegerTuple2._1().split("<=>")[0]), new Tuple2<String, Integer>(stringIntegerTuple2._1().split("<=>")[1], stringIntegerTuple2._2()));
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<Integer, Iterable<Tuple2<String, Integer>>>, Integer, Map<String, Integer>>() {
            @Override
            public Tuple2<Integer, Map<String, Integer>> call(Tuple2<Integer, Iterable<Tuple2<String, Integer>>> integerIterableTuple2) throws Exception {
                Map<String, Integer> map = new HashMap<String, Integer>();
                for (Tuple2<String, Integer> t : integerIterableTuple2._2()) {
                    if (!map.containsKey(t._1()))
                        map.put(t._1(), t._2());
                }
                return new Tuple2<Integer, Map<String, Integer>>(integerIterableTuple2._1(), map);
            }
        }).collectAsMap();


        //计算互信息
        double result = 0.0;
        for (Integer data : apache_p_date.keySet()){
            for (String x : apache_X_date.keySet()){
                double pxy = 0.0;
                if (numberOfXindata.containsKey(data) && numberOfXindata.get(data).containsKey(x))
                { pxy = 1.0 * numberOfXindata.get(data).get(x)/data_n.get(data);
                    result += pxy * Math.log(1.0 * pxy / apache_X_date.get(x));}
            }
        }

        System.out.println("互信息 : " + result);

        //检验url和data字段是否一一对应
        JavaPairRDD<String, Integer> apacheLog_url_differentdata = apacheLogRDD.filter(new Function<ApacheLog, Boolean>() {
            @Override
            public Boolean call(ApacheLog apacheLog) throws Exception {
                if (apacheLog.getData() == -1)
                    return false;
                return true;
            }
        }).map(new Function<ApacheLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(ApacheLog apacheLog) throws Exception {
                return new Tuple2<String, Integer>(apacheLog.getUrl(), apacheLog.getData());
            }
        }).distinct().mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<String, Integer>(stringIntegerTuple2._1(), 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._2() != 1;
            }
        });

        //有多少url具有不同的data，will print 40983
        System.out.println("有多少url具有不同的data : "+apacheLog_url_differentdata.count());

        DataFrame apacheLogDF = spark.createDataFrame(apacheLogRDD, ApacheLog.class);
        apacheLogDF.registerTempTable("apacheLog");

        //data字段平均值
        DataFrame apacheLog_data_avg = spark.sql("SELECT AVG(data) FROM apacheLog where data <> -1");

        //data字段最大值
        DataFrame apacheLog_data_max = spark.sql("SELECT MAX(data) FROM apacheLog where data <> -1");

        //data字段最小值
        DataFrame apacheLog_data_min = spark.sql("SELECT min(data) FROM apacheLog where data <> -1");

        //data字段众数
        DataFrame apacheLog_data_most = spark.sql("SELECT data, count(*) FROM apacheLog where data <> -1 GROUP BY data ORDER BY count(*) desc");

        // 发生缺失的日志数
         System.out.println(apacheLogDF.select("ip").where("data=-1").count());

        //存在于字典中的缺失数据
        DataFrame apacheLog_url_data = spark.sql("SELECT url,data FROM apacheLog where data=-1 and url in (SELECT distinct url FROM apacheLog WHERE data<>-1)");

        System.out.println("存在于字典中的缺失数据数量 ： "+apacheLog_url_data.count());

        //构建缺失数据的查询字典
        DataFrame apacheLog_data_notnull = spark.sql("SELECT distinct url,data FROM apacheLog WHERE data<>-1");
        JavaPairRDD<String, Integer> urlDataHashMapRDD = apacheLog_data_notnull.toJavaRDD()
                .map(new Function<Row, String>() {
                    @Override
                    public String call(Row row) throws Exception {
                        return row.<String>get(0) + "," + row.<Integer>get(1);
                    }
                })
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        int index = s.lastIndexOf(",");
                        return new Tuple2<String, Integer>(s.substring(0, index), Integer.parseInt(s.substring(index + 1)));
                    }
                }).cache();

        final Map<String, Integer> urlDataHashMap = urlDataHashMapRDD.collectAsMap();


        final Broadcast<Map<String, Integer>> urlDataHashMap_B = jSpark.broadcast(urlDataHashMap);


        //利用字典补上空缺值
        JavaRDD<ApacheLog> apacheLog_ClearnedByHashMap = apacheLogRDD.map(new Function<ApacheLog, ApacheLog>() {
            @Override
            public ApacheLog call(ApacheLog apacheLog) throws Exception {

                if (apacheLog.getData() == -1) {
                    Map<String, Integer> map = urlDataHashMap_B.value();
                    if (map.containsKey(apacheLog.getUrl()))
                        apacheLog.setData(map.get(apacheLog.getUrl()));
                }

                return apacheLog;
            }
        }).cache();


        System.out.println(apacheLog_ClearnedByHashMap.filter(new Function<ApacheLog, Boolean>() {
            @Override
            public Boolean call(ApacheLog apacheLog) throws Exception {
                return apacheLog.getData() == -1 ? true : false;
            }
        }).count());
    }
}
