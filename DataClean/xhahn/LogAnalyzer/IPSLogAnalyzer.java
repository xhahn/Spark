package com.xhahn.LogAnalyzer;

import com.xhahn.Clearner.IPSClearner;
import com.xhahn.JavaBean.IPSLog;
import com.xhahn.LogParser.IPSParser;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * User: xhahn
 * Data: 2017/1/18/0018
 * Time: 17:28
 */
public class IPSLogAnalyzer implements Serializable {

    private static volatile IPSLogAnalyzer instance;
    private static volatile Accumulator<Integer> format_accumulator;

    private IPSLogAnalyzer() {
    }

    public static IPSLogAnalyzer getIPSLogAnalyzer() {
        if (instance == null) {
            synchronized (IPSLogAnalyzer.class) {
                if (instance == null)
                    instance = new IPSLogAnalyzer();
            }
        }

        return instance;
    }

    public void runIPSLogFormatAndContentClearner(JavaSparkContext spark, String sourcePath, String outputPath) {

        //ͳ计算格式错误的记录数，结果为29990
        format_accumulator = spark.accumulator(0, "format");
        JavaRDD<String> ipsLog_format = spark.textFile(sourcePath)
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String log) throws Exception {
                        IPSClearner ipsCleaner = IPSClearner.getIPSClearner();
                        boolean result = ipsCleaner.isCompleted(log);
                        if (result == false)
                            format_accumulator.add(1);
                        return result;
                    }
                }).cache();

        ipsLog_format.count();

        JavaRDD<IPSLog> ipsLogRDD = ipsLog_format.map(new Function<String, IPSLog>() {
            @Override
            public IPSLog call(String log) throws Exception {
                IPSParser ipsParser = IPSParser.getIPSParser();
                IPSLog ipsLog = ipsParser.parse(log);

                return ipsLog;
            }
        }).cache();

        JavaRDD<IPSLog> ips_accept = ipsLogRDD.filter(new Function<IPSLog, Boolean>() {
            @Override
            public Boolean call(IPSLog ipsLog) throws Exception {
                if (ipsLog.getAction().equals("1"))
                    return true;
                else
                    return false;
            }
        });

        //action=accept，输出1454837
        System.out.println("action=accept : "+ips_accept.count());

        JavaRDD<IPSLog> ipsLogRDD_constructed = featureConstruct(ipsLogRDD);
        //将类别型特征转化为数值型特征

        //输出
        ipsLogRDD_constructed.repartition(1).map(new Function<IPSLog, String>() {
            @Override
            public String call(IPSLog ipsLog) throws Exception {
                return ipsLog.toString();
            }
        }).saveAsTextFile(outputPath);

        SQLContext sqlSpark = new SQLContext(spark);
        DataFrame ipsLogDF = sqlSpark.createDataFrame(ipsLogRDD_constructed, IPSLog.class);
        ipsLogDF.registerTempTable("ipsLog");

        sqlSpark.sql("select distinct action, type, subtype, level, vd from ipsLog").show();

    }

    private JavaRDD<IPSLog> featureConstruct(JavaRDD<IPSLog> ipsLogRDD) {
        Set<String> features = new HashSet<>();
        Map<String, HashMap<String, Integer>> fmap = new HashMap<>();
        features.add("type");
        features.add("subtype");
        features.add("level");
        features.add("vd");


        int j = 1;
        String name = "type";
        for (String s : ipsLogRDD.map(new Function<IPSLog, String>() {
            @Override
            public String call(IPSLog ipsLog) throws Exception {
                return ipsLog.getType();
            }
        }).distinct().collect()) {
            if (fmap.containsKey(name)) {
                HashMap<String, Integer> map = fmap.get(name);
                map.put(s, j);
                fmap.put(name, map);
                j++;
            } else {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(s, j);
                j++;
                fmap.put(name, map);
            }
        }

        j = 1;
        name = "subtype";

        for (String s : ipsLogRDD.map(new Function<IPSLog, String>() {
            @Override
            public String call(IPSLog ipsLog) throws Exception {
                return ipsLog.getSubtype();
            }
        }).distinct().collect()) {
            if (fmap.containsKey(name)) {
                HashMap<String, Integer> map = fmap.get(name);
                map.put(s, j);
                fmap.put(name, map);
                j++;
            } else {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(s, j);
                j++;
                fmap.put(name, map);
            }
        }

        j = 1;
        name = "level";
        for (String s : ipsLogRDD.map(new Function<IPSLog, String>() {
            @Override
            public String call(IPSLog ipsLog) throws Exception {
                return ipsLog.getLevel();
            }
        }).distinct().collect()) {
            if (fmap.containsKey(name)) {
                HashMap<String, Integer> map = fmap.get(name);
                map.put(s, j);
                fmap.put(name, map);
                j++;
            } else {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(s, j);
                j++;
                fmap.put(name, map);
            }
        }

        j = 1;
        name = "vd";
        for (String s : ipsLogRDD.map(new Function<IPSLog, String>() {
            @Override
            public String call(IPSLog ipsLog) throws Exception {
                return ipsLog.getVd();
            }
        }).distinct().collect()) {
            if (fmap.containsKey(name)) {
                HashMap<String, Integer> map = fmap.get(name);
                map.put(s, j);
                fmap.put(name, map);
                j++;
            } else {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(s, j);
                j++;
                fmap.put(name, map);
            }
        }


        final Map<String, HashMap<String, Integer>> fmap_f = fmap;

        ipsLogRDD = ipsLogRDD.map(new Function<IPSLog, IPSLog>() {
            @Override
            public IPSLog call(IPSLog ipsLog) throws Exception {
                String v = ipsLog.getType();
                Integer value = fmap_f.get("type").get(v);
                ipsLog.setType(value + "");
                return ipsLog;
            }
        });
        ipsLogRDD = ipsLogRDD.map(new Function<IPSLog, IPSLog>() {
            @Override
            public IPSLog call(IPSLog ipsLog) throws Exception {
                String v = ipsLog.getVd();
                Integer value = fmap_f.get("vd").get(v);
                ipsLog.setVd(value + "");
                return ipsLog;
            }
        });
        ipsLogRDD = ipsLogRDD.map(new Function<IPSLog, IPSLog>() {
            @Override
            public IPSLog call(IPSLog ipsLog) throws Exception {
                String v = ipsLog.getSubtype();
                Integer value = fmap_f.get("subtype").get(v);
                ipsLog.setSubtype(value + "");
                return ipsLog;
            }
        });
        ipsLogRDD = ipsLogRDD.map(new Function<IPSLog, IPSLog>() {
            @Override
            public IPSLog call(IPSLog ipsLog) throws Exception {
                String v = ipsLog.getLevel();
                Integer value = fmap_f.get("level").get(v);
                ipsLog.setLevel(value + "");
                return ipsLog;
            }
        });



        return ipsLogRDD;
    }
}
