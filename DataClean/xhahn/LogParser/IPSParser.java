package com.xhahn.LogParser;

import com.xhahn.JavaBean.IPSLog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: xhahn
 * Data: 2017/1/11/0011
 * Time: 21:49
 */
public class IPSParser {
    private static volatile IPSParser instance;

    final String logREX_1 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=([^\\,]+)\\,srcip=([^\\,]+)\\,srcport=([^\\,]+)\\,srcintf=\\\"([^\\,]+)\\\"\\,dstip=([^\\,]+)\\,dstport=([^\\,]+)\\,dstintf=\\\"([^\\,]+)\\\"\\,poluuid=([^\\,]+)\\,sessionid=([^\\,]+)\\,proto=([^\\,]+)\\,action=([^\\,]+)\\,policyid=([^\\,]+)\\,dstcountry=\\\"([^\\\"]+)\\\"\\,srccountry=\\\"([^\\\"]+)\\\"\\,trandisp=([^\\,]+)\\,service=[\\\"]+([^\\,]+)\\\"\\,duration=([^\\,]+)\\,sentbyte=([^\\,]+)\\,rcvdbyte=([^\\,]+)\\,sentpkt=([^\\,]+)\\,rcvdpkt=([^\\,]+)\\,appcat=[\"]+([^\"]+)\\\"";
    final Pattern pattern_1 = Pattern.compile(logREX_1);
    final String logREX_2 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,eventtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=\\\"([^\\,]+)\\\"\\,severity=([^\\,]+)\\,srcip=([^\\,]+)\\,dstip=([^\\,]+)\\,sessionid=([^\\,]+)\\,action=([^\\,]+)\\,proto=([^\\,]+)\\,service=([^\\,]+)\\,attack=\\\"([^\\,]+)\\\"\\,srcport=([^\\,]+)\\,dstport=([^\\,]+)\\,direction=([^\\,]+)\\,attackid=([^\\,]+)\\,profile=\\\"([^\\,]+)\\\"\\,ref=\\\"([^\\,]+)\\\"\\,incidentserialno=([^\\,]+)\\,msg=\\\"([^\"]+)\\\",crscore=(\\d+)\\,crlevel=(\\S+)";
    final Pattern pattern_2 = Pattern.compile(logREX_2);
    final String logREX_3 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=([^\\,]+)\\,srcip=([^\\,]+)\\,srcport=([^\\,]+)\\,srcintf=\\\"([^\\,]+)\\\"\\,dstip=([^\\,]+)\\,dstport=([^\\,]+)\\,dstintf=\\\"([^\\,]+)\\\"\\,poluuid=([^\\,]+)\\,sessionid=([^\\,]+)\\,proto=([^\\,]+)\\,action=([^\\,]+)\\,policyid=([^\\,]+)\\,appcat=[\"]+([^\"]+)";
    final Pattern pattern_3 = Pattern.compile(logREX_3);
    final String logREX_4 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=([^\\,]+)\\,srcip=([^\\,]+)\\,srcport=([^\\,]+)\\,srcintf=\\\"([^\\,]+)\\\"\\,dstip=([^\\,]+)\\,dstport=([^\\,]+)\\,dstintf=\\\"([^\\,]+)\\\"\\,poluuid=([^\\,]+)\\,sessionid=([^\\,]+)\\,proto=([^\\,]+)\\,action=([^\\,]+)\\,policyid=([^\\,]+)\\,dstcountry=\\\"([^\\\"]+)\\\"\\,srccountry=\\\"([^\\\"]+)\\\"\\,trandisp=([^\\,]+)\\,service=[\\\"]+([^\\,]+)\\\"\\,duration=([^\\,]+)\\,sentbyte=([^\\,]+)\\,rcvdbyte=([^\\,]+)\\,sentpkt=([^\\,]+)\\,appcat=[\"]+([^\"]+)";
    final Pattern pattern_4 = Pattern.compile(logREX_4);
    final String logREX_5 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=\\\"([^\\,]+)\\\"\\,severity=([^\\,]+)\\,srcip=([^\\,]+)\\,dstip=([^\\,]+)\\,srcintf=\\\"([^\\,]+)\\\"\\,sessionid=([^\\,]+)\\,action=([^\\,]+)\\,proto=([^\\,]+)\\,service=([^\\,]+)\\,count=([^\\,]+)\\,attack=\\\"([^\\,]+)\\\"\\,srcport=([^\\,]+)\\,dstport=([^\\,]+)\\,attackid=([^\\,]+)\\,profile=\\\"([^\\,]+)\\\"\\,ref=\\\"([^\\,]+)\\\"\\,msg=\\\"([^\"]+)\\\"\\,crscore=(\\d+)\\,crlevel=(\\S+)";
    final Pattern pattern_5 = Pattern.compile(logREX_5);



    private IPSParser() {
    }

    public static IPSParser getIPSParser() {
        if (instance == null) {
            synchronized (IPSParser.class) {
                if (instance == null)
                    instance = new IPSParser();
            }
        }

        return instance;
    }

    /**
     * @param log Dec 30 13:59:29 115.156.191.116 date=2016-12-30,time=13:59:22,devname=FG3K6C3A13800359,devid=FG3K6C3A13800359,logid=0000000013,type=traffic,subtype=forward,level=notice,vd=dc,srcip=122.205.9.67,srcport=37525,srcintf="port25",dstip=202.114.0.131,dstport=53,dstintf="port15",poluuid=0e772a04-6d75-51e5-3cf6-d18e86f90bac,sessionid=412623881,proto=17,action=accept,policyid=6,dstcountry="China",srccountry="China",trandisp=noop,service="DNS",duration=50,sentbyte=124,rcvdbyte=274,sentpkt=2,rcvdpkt=2,appcat="unscanned"
     * @return IPSLog
     */
    public IPSLog parse(String log) {
        String date = "";
        String time = "";
        Integer logid = 0;
        String type = "";
        String subtype = "";
        String level = "" ;
        String vd = "";
        String srcip = "";
        Integer srcport = 0;
        String dstip = "";
        Integer dstport = 0;
        String sessionid = "";
        Integer proto = 0;
        String action = "";

        IPSLog ipsLog = new IPSLog();

        Matcher matcher_1 = pattern_1.matcher(log);
        if (matcher_1.find()) {
            date = matcher_1.group(3);
            time = matcher_1.group(4);
            logid = Integer.parseInt(matcher_1.group(7));
            type = matcher_1.group(8);
            subtype = matcher_1.group(9);
            level = matcher_1.group(10);
            vd = matcher_1.group(11);
            srcip = matcher_1.group(12);
            srcport = Integer.parseInt(matcher_1.group(13));
            dstip = matcher_1.group(15);
            dstport = Integer.parseInt(matcher_1.group(16));
            sessionid = matcher_1.group(19);
            proto = Integer.parseInt(matcher_1.group(20));
            action = matcher_1.group(21);
        }
        else {
            Matcher matcher_3 = pattern_3.matcher(log);
            if (matcher_3.find()) {
                date = matcher_3.group(3);
                time = matcher_3.group(4);
                logid = Integer.parseInt(matcher_3.group(7));
                type = matcher_3.group(8);
                subtype = matcher_3.group(9);
                level = matcher_3.group(10);
                vd = matcher_3.group(11);
                srcip = matcher_3.group(12);
                srcport = Integer.parseInt(matcher_3.group(13));
                dstip = matcher_3.group(15);
                dstport = Integer.parseInt(matcher_3.group(16));
                sessionid = matcher_3.group(19);
                proto = Integer.parseInt(matcher_3.group(20));
                action = matcher_3.group(21);
            }
            else {
                Matcher matcher_4 = pattern_4.matcher(log);
                if (matcher_4.find()) {
                    date = matcher_4.group(3);
                    time = matcher_4.group(4);
                    logid = Integer.parseInt(matcher_4.group(7));
                    type = matcher_4.group(8);
                    subtype = matcher_4.group(9);
                    level = matcher_4.group(10);
                    vd = matcher_4.group(11);
                    srcip = matcher_4.group(12);
                    srcport = Integer.parseInt(matcher_4.group(13));
                    dstip = matcher_4.group(15);
                    dstport = Integer.parseInt(matcher_4.group(16));
                    sessionid = matcher_4.group(19);
                    proto = Integer.parseInt(matcher_4.group(20));
                    action = matcher_4.group(21);
                }
                else {
                    Matcher matcher_5 = pattern_5.matcher(log);
                    if (matcher_5.find()) {
                        date = matcher_5.group(3);
                        time = matcher_5.group(4);
                        logid = Integer.parseInt(matcher_5.group(7));
                        type = matcher_5.group(8);
                        subtype = matcher_5.group(9);
                        level = matcher_5.group(10);
                        vd = matcher_5.group(11);
                        srcip = matcher_5.group(13);
                        srcport = Integer.parseInt(matcher_5.group(22));
                        dstip = matcher_5.group(14);
                        dstport = Integer.parseInt(matcher_5.group(23));
                        sessionid = matcher_5.group(16);
                        proto = Integer.parseInt(matcher_5.group(18));
                        action = matcher_5.group(17);
                    }
                    else {
                        Matcher matcher_2 = pattern_2.matcher(log);
                        if (matcher_2.find()) {
                            date = matcher_2.group(3);
                            time = matcher_2.group(4);
                            logid = Integer.parseInt(matcher_2.group(7));
                            type = matcher_2.group(8);
                            subtype = matcher_2.group(9);
                            level = matcher_2.group(11);
                            vd = matcher_2.group(12);
                            srcip = matcher_2.group(14);
                            srcport = Integer.parseInt(matcher_2.group(21));
                            dstip = matcher_2.group(15);
                            dstport = Integer.parseInt(matcher_2.group(22));
                            sessionid = matcher_2.group(16);
                            proto = Integer.parseInt(matcher_2.group(18));
                            action = matcher_2.group(17);
                        }
                    }
                }
            }
        }

        //提取并转化字段
        String date_r = "";
        for (String s : date.split("-"))
        {
            date_r += s;
        }
        String time_r = "";
        for (String s : time.split(":"))
        {
            time_r += s;
        }

        srcip = construction(srcip);
        dstip = construction(dstip);

        if (action.equals("accept"))
        {
            action = "1";
        }
        else {
            action = "0";
        }

        ipsLog.setAction(action).setDstport(dstport).setDate(date_r).setDstip(dstip).setLevel(level).setLogid(logid).setProto(proto).setSessionid(sessionid).setSrcip(srcip).setSrcport(srcport).setSubtype(subtype).setTime(time_r).setType(type).setVd(vd);


        return ipsLog;
    }

    private String construction(String src) {
        String parts[] = src.split("\\.");
        if (parts.length<4)
            System.out.println(src);
        long x = 1;
        long result = 0;
        for (int i=3;i>=0;i--){
            result += Integer.parseInt(parts[i])*x;
            x *= 255;
        }
        return result+"";
    }


}
