package com.xhahn.LogParser;

import com.xhahn.JavaBean.ApacheLog;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * User: xhahn
 * Data: 2017/1/16/0016
 * Time: 16:16
 */
public class ApacheParser implements Serializable {

    private static volatile ApacheParser instance;

    private ApacheParser() {
    }

    public static ApacheParser getApacheParser() {
        if (instance == null) {
            synchronized (ApacheParser.class) {
                if (instance == null)
                    instance = new ApacheParser();
            }
        }

        return instance;
    }


    /**
     * @param log 58.48.28.54 - - [27/Dec/2016:00:11:48 +0800] "GET /coremail/XJS/images/small_icons.gif HTTP/1.1" 200 18603
     * @return ApacheLog
     * @throws ParseException
     */
    public ApacheLog parse(String log) throws ParseException {

        //获取ip
        String[] parts = log.split(" ");
        String ip = parts[0];

        // 获取时间,并转换格式
        String time;
        int getTimeFirst = log.indexOf("[");
        int getTimeLast = log.indexOf("]");
        time = log.substring(getTimeFirst + 1, getTimeLast).trim();
        Date date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US).parse(time);

        //获取http方法和url
        String httpMethod = null;
        String url = null;
        String httpProtocol = null;
        try {
            String getURL = log.split("\"")[1];
            httpMethod = getURL.split(" ")[0];
            url = getURL.split(" ")[1];
            httpProtocol = getURL.split(" ")[2];
        } catch (ArrayIndexOutOfBoundsException e) {

        }
        //获取状态码
        Integer statue = -1;
        if (!parts[8].equals("-"))
            statue = Integer.parseInt(parts[8]);

        //获取数据量
        Integer data = -1;
        if (!parts[9].equals("-"))
            data = Integer.parseInt(parts[9]);

        ApacheLog apacheLog = new ApacheLog();
        apacheLog.setIp(ip).setDate(new Timestamp(date.getTime())).setHttpMethod(httpMethod).setUrl(url).setStatue(statue).setData(data).setHttpProtocol(httpProtocol);

        return apacheLog;
    }

    public ApacheLog parseClearnLog(String log) throws ParseException {
        String[] parts = log.split("<=>");
        ApacheLog apacheLog = new ApacheLog();

        try {
            apacheLog.setIp(parts[0]).setDate(Timestamp.valueOf(parts[1])).setHttpMethod(parts[2]).setUrl(parts[3]).setHttpProtocol(parts[4]).setStatue(Integer.parseInt(parts[5])).setData(Integer.parseInt(parts[6]));
        }catch(IllegalArgumentException e){
            System.out.println(log+" "+parts[1]);
        }
        return apacheLog;
    }

}
