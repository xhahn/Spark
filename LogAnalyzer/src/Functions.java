package com.xhahn.logs;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.regex.Matcher;

import java.io.Serializable;
import java.util.Comparator;
import java.util.regex.Pattern;

public class Functions {

    public static Function2<Long, Long, Long> SUM_REDUCER =
            new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long a, Long b) throws Exception {
                    return a + b;
                }
            };

    public static class ValueComparator<K, V>
            implements Comparator<Tuple2<K, V>>, Serializable {
        private Comparator<V> comparator;

        public ValueComparator(Comparator<V> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
            return comparator.compare(o1._2(), o2._2());
        }
    }

    public static class LongComparator
            implements Comparator<Long>, Serializable {

        @Override
        public int compare(Long a, Long b) {
            if (a > b) return 1;
            if (a.equals(b)) return 0;
            return -1;
        }
    }


    public static Comparator<Long> LONG_NATURAL_ORDER_COMPARATOR =
            new LongComparator();

    public static Function<String, ApacheAccessLog> PARSE_LOG_LINE =
            new Function<String, ApacheAccessLog>() {
                @Override
                public ApacheAccessLog call(String logline) throws Exception {
                    return ApacheAccessLog.parseFromLogLine(logline);
                }
            };

    public static Function<ApacheAccessLog, Long> GET_CONTENT_SIZE =
            new Function<ApacheAccessLog, Long>() {
                @Override
                public Long call(ApacheAccessLog apacheAccessLog) throws Exception {
                    return apacheAccessLog.getContentSize();
                }
            };

    public static PairFunction<ApacheAccessLog, Integer, Long> GET_RESPONSE_CODE =
            new PairFunction<ApacheAccessLog, Integer, Long>() {
                @Override
                public Tuple2<Integer, Long> call(ApacheAccessLog log)
                        throws Exception {
                    return new Tuple2<Integer, Long>(log.getResponseCode(), 1L);
                }
            };

    public static PairFunction<ApacheAccessLog, String, Long> GET_IP_ADDRESS =
            new PairFunction<ApacheAccessLog, String, Long>() {
                @Override
                public Tuple2<String, Long> call(ApacheAccessLog log) throws Exception {
                    return new Tuple2<String, Long>(log.getIpAddress(), 1L);
                }
            };
    public static PairFunction<Row, Integer, Long> GET_sqlRESPONSE_CODE =
            new PairFunction<Row, Integer, Long>() {
                @Override
                public Tuple2<Integer, Long> call(Row row) throws Exception {
                    return new Tuple2<Integer, Long>(row.getInt(0), row.getLong(1));
                }
            };

    public static Function<Tuple2<String, Long>, Boolean> FILTER_GREATER_10 =
            new Function<Tuple2<String, Long>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                    return tuple._2() > 10;
                }
            };
    public static Function<String, Boolean> FILTER_AccessLog =
            new Function<String, Boolean>() {
                private static final String LOG_ENTRY_PATTERN =
                        // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
                        "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
                private final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

                @Override
                public Boolean call(String s) throws Exception {
                    Matcher mather = PATTERN.matcher(s);
                    return mather.find();
                }

            };
    public static Function<Tuple2<String, Long>, String> GET_TUPLE_FIRST =
            new Function<Tuple2<String, Long>, String>() {
                @Override
                public String call(Tuple2<String, Long> tuple) throws Exception {
                    return tuple._1();
                }
            };

    public static PairFunction<ApacheAccessLog, String, Long> GET_ENDPOINT =
            new PairFunction<ApacheAccessLog, String, Long>() {
                @Override
                public Tuple2<String, Long> call(ApacheAccessLog log) throws Exception {
                    return new Tuple2<String, Long>(log.getEndpoint(), 1L);
                }
            };
}