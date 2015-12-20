package com.xhahn.gmm;

import com.xhahn.gmm.LogAnalyzer.AccessLogFile;
import com.xhahn.gmm.LogAnalyzer.AccessOutFile;
import com.xhahn.gmm.LogAnalyzer.LogAnalyzer;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: xhahn
 * Data: 2015/8/26/0026
 * Time: 10:44
 */
public class Functions {

    public static Function<String, Vector> PARSE_VECTOR =
            new Function<String, Vector>() {
                private final Pattern space = Pattern.compile(",");

                public Vector call(String s) throws Exception {
                    String[] words = space.split(s);
                    double[] point = new double[words.length];
                    for (int i = 0; i < words.length; ++i) {
                        point[i] = Double.parseDouble(words[i]);
                    }
                    return Vectors.dense(point);
                }
            };
    public static Function<double[],Vector> PARSE_VECTOR2 =
            new Function<double[], Vector>() {
                @Override
                public Vector call(double[] doubles) throws Exception {
                    return Vectors.dense(doubles);
                }
            };

    public static Function<String, Boolean> FILTER_ACCESSLOGFILE =
            new Function<String, Boolean>() {
                private static final String LOGPATTERN =
                        "^(\\S+),(\\S+),(\\S+),(\\S+),([\\S+]+\\s[\\d+\\:]+\\.\\d+),([\\S+]+\\s[\\d+\\:]+\\.\\d+),(\\d+),(\\d+),(\\S+),(\\S+),(\\S+)";
                private final Pattern PATTERN = Pattern.compile(LOGPATTERN);

                public Boolean call(String s) throws Exception {
                    Matcher m = PATTERN.matcher(s);
                    return m.find();
                }
            };

    public static Function<String, AccessLogFile> PARSE_ACCESSLOGFILE =
            new Function<String, AccessLogFile>() {
                public AccessLogFile call(String s) throws Exception {
                    return AccessLogFile.parseAccessLogFile(s);
                }
            };
    public static Function<String,AccessOutFile> PARSE_ACCESSOUTFILE =
            new Function<String, AccessOutFile>() {
                @Override
                public AccessOutFile call(String s) throws Exception {
                    return AccessOutFile.parseAccessOutFile(s);
                }
            };

    public static Function<AccessLogFile, String> GET_ONLINE_SEC =
            new Function<AccessLogFile, String>() {
                public String call(AccessLogFile accessLogFile) throws Exception {
                    return accessLogFile.getONLINE_SEC();
                }
            };

    public static Function<String, Boolean> FILTER_ONLINE_SEC_LESS10 =
            new Function<String, Boolean>() {
                public Boolean call(String s) throws Exception {
                    return Integer.parseInt(s) < 10;
                }
            };

    public static VoidFunction<AccessLogFile> ACCUMULATE_TIME =
            new VoidFunction<AccessLogFile>() {
                @Override
                public void call(AccessLogFile accessLogFile) throws Exception {
                    Map<String, int[]> studentsTime = LogAnalyzer.studentsTime;
                    int totalTime = Integer.parseInt(accessLogFile.getONLINE_SEC());
                    String inTime = accessLogFile.getLOGIN_TIME();
                    String outTime = accessLogFile.getLOGOUT_TIME();
                    String ID = accessLogFile.getUSER_ID();
                    //计算每条记录在各个时间段内的连接时长
                    int[] time = accumTime(inTime, outTime, totalTime);

                    //若该同学已经存在，则叠加到原先的时长，否则新建记录，添加到map
                    if (studentsTime.containsKey(ID)) {
                        int[] temp = studentsTime.get(ID);
                        for (int i = 0; i < 12; i++) {
                            temp[i] += time[i];
                            studentsTime.put(ID, temp);
                        }
                    } else {
                        studentsTime.put(ID, time);
                    }
                }
            };
    public static VoidFunction<AccessOutFile> ACCUMULATE_OUTPUT =
            new VoidFunction<AccessOutFile>() {
                @Override
                public void call(AccessOutFile accessOutFile) throws Exception {
                    int[] temp = accessOutFile.getCoordinates();
                    int i = 0;

                    while(i<temp.length){
                        LogAnalyzer.outPut[LogAnalyzer.count][i] = (double)temp[i];
                        i++;
                }
                    LogAnalyzer.count++;
                }
            };


    public static Function<AccessLogFile,Boolean> FILTER_ONLINE_SEC_OVER10 =
            new Function<AccessLogFile, Boolean>() {
                @Override
                public Boolean call(AccessLogFile accessLogFile) throws Exception {
                    return Integer.parseInt(accessLogFile.getONLINE_SEC())>10;
                }
            };


    /**
     * @param inTime    开始时间
     * @param outTime   结束时间
     * @param totalTime 连接总时长
     * @return 各时间段连接时长
     */
    private static int[] accumTime(String inTime, String outTime, int totalTime) {
        int[] time = new int[12];
        int index;
        int[] intime = new int[3];
        //System.out.println(inTime);
        //inTime格式：20xx-xx-xx xx:xx:xx
        intime[0] = Integer.parseInt(inTime.substring(11, 13));//时
        intime[1] = Integer.parseInt(inTime.substring(14, 16));//分
        intime[2] = Integer.parseInt(inTime.substring(17, 19));//秒
        //System.out.println(inTime+" "+intime[0]+" "+intime[1]+" "+intime[2]);
        int[] outtime = new int[3];
        outtime[0] = Integer.parseInt(outTime.substring(11, 13));
        outtime[1] = Integer.parseInt(outTime.substring(14, 16));
        outtime[2] = Integer.parseInt(outTime.substring(17, 19));

        index = intime[0] / 2;
        //System.out.println(intime[0]+" "+index+" "+outtime[0]+" "+totalTime);
        //当持续时间长度超过一个时间段时，先记录下在第一个时间段的持续时间
        if (outtime[0] / 2 > index) {
            time[index] = (index * 2 + 1 - intime[0]) * 60 * 60 + (59 - intime[1]) * 60 + 60 - intime[2];
            totalTime -= time[index];
            index++;
        }

        while (totalTime != 0) {
            if (totalTime >= 7200) {
                time[index] = 7200;
                totalTime -= 7200;
                index = (index + 1) % 12; //若有人从前一天晚上到第二天凌晨一直在线，则结束时间比开始时间小，造成数组溢出
            } else {
                time[index] = totalTime;
                totalTime = 0;
            }
        }


        return time;

    }

    public static void storeFile(Set<Map.Entry<String, int[]>> entrySet, String filePath) {
        Iterator iterator;
        File tempFile;
        FileWriter fw = null;
        BufferedWriter writer = null;

        tempFile = new File(filePath);
        //文件不存在则创建文件
        if (!tempFile.exists()) {
            try {
                tempFile.createNewFile();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        iterator = entrySet.iterator();

        //写入文件
        try {
            fw = new FileWriter(tempFile);
            writer = new BufferedWriter(fw);
            while (iterator.hasNext()) {
                Map.Entry<String, int[]> entry = (Map.Entry<String, int[]>) iterator.next();
                writer.write(entry.getKey());
                for (int i : entry.getValue()) {
                    writer.write(" " + i);
                }
                writer.newLine();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
