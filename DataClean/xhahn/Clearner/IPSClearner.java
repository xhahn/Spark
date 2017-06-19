package com.xhahn.Clearner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: xhahn
 * Data: 2017/1/16/0016
 * Time: 22:11
 */
public class IPSClearner {

    private static volatile IPSClearner instance;

    final String logREX_1 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=([^\\,]+)\\,srcip=([^\\,]+)\\,srcport=([^\\,]+)\\,srcintf=\\\"([^\\,]+)\\\"\\,dstip=([^\\,]+)\\,dstport=([^\\,]+)\\,dstintf=\\\"([^\\,]+)\\\"\\,poluuid=([^\\,]+)\\,sessionid=([^\\,]+)\\,proto=([^\\,]+)\\,action=([^\\,]+)\\,policyid=([^\\,]+)\\,dstcountry=\\\"([^\\\"]+)\\\"\\,srccountry=\\\"([^\\\"]+)\\\"\\,trandisp=([^\\,]+)\\,service=[\\\"]+([^\\,]+)\\\"\\,duration=([^\\,]+)\\,sentbyte=([^\\,]+)\\,rcvdbyte=([^\\,]+)\\,sentpkt=([^\\,]+)\\,rcvdpkt=([^\\,]+)\\,appcat=[\"]+([^\"]+)\\\"";
    final Pattern pattern_1 = Pattern.compile(logREX_1);
    final String logREX_2 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=([^\\,]+)\\,srcip=([^\\,]+)\\,srcport=([^\\,]+)\\,srcintf=\\\"([^\\,]+)\\\"\\,dstip=([^\\,]+)\\,dstport=([^\\,]+)\\,dstintf=\\\"([^\\,]+)\\\"\\,poluuid=([^\\,]+)\\,sessionid=([^\\,]+)\\,proto=([^\\,]+)\\,action=([^\\,]+)\\,policyid=([^\\,]+)\\,appcat=[\"]+([^\"]+)";
    final Pattern pattern_2 = Pattern.compile(logREX_2);
    final String logREX_3 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=([^\\,]+)\\,srcip=([^\\,]+)\\,srcport=([^\\,]+)\\,srcintf=\\\"([^\\,]+)\\\"\\,dstip=([^\\,]+)\\,dstport=([^\\,]+)\\,dstintf=\\\"([^\\,]+)\\\"\\,poluuid=([^\\,]+)\\,sessionid=([^\\,]+)\\,proto=([^\\,]+)\\,action=([^\\,]+)\\,policyid=([^\\,]+)\\,dstcountry=\\\"([^\\\"]+)\\\"\\,srccountry=\\\"([^\\\"]+)\\\"\\,trandisp=([^\\,]+)\\,service=[\\\"]+([^\\,]+)\\\"\\,duration=([^\\,]+)\\,sentbyte=([^\\,]+)\\,rcvdbyte=([^\\,]+)\\,sentpkt=([^\\,]+)\\,appcat=[\"]+([^\"]+)";
    final Pattern pattern_3 = Pattern.compile(logREX_3);
    final String logREX_4 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=\\\"([^\\,]+)\\\"\\,severity=([^\\,]+)\\,srcip=([^\\,]+)\\,dstip=([^\\,]+)\\,srcintf=\\\"([^\\,]+)\\\"\\,sessionid=([^\\,]+)\\,action=([^\\,]+)\\,proto=([^\\,]+)\\,service=([^\\,]+)\\,count=([^\\,]+)\\,attack=\\\"([^\\,]+)\\\"\\,srcport=([^\\,]+)\\,dstport=([^\\,]+)\\,attackid=([^\\,]+)\\,profile=\\\"([^\\,]+)\\\"\\,ref=\\\"([^\\,]+)\\\"\\,msg=\\\"([^\"]+)\\\"\\,crscore=(\\d+)\\,crlevel=(\\S+)";
    final Pattern pattern_4 = Pattern.compile(logREX_4);
    final String logREX_5 = "^(\\S+\\s+\\d+\\s+[\\d+:/]+\\d) (\\S+) date=([\\d+-]+)\\,time=([\\d+:]+)\\,devname=([^\\,]+)\\,devid=([^\\,]+)\\,logid=([^\\,]+)\\,type=([^\\,]+)\\,subtype=([^\\,]+)\\,eventtype=([^\\,]+)\\,level=([^\\,]+)\\,vd=\\\"([^\\,]+)\\\"\\,severity=([^\\,]+)\\,srcip=([^\\,]+)\\,dstip=([^\\,]+)\\,sessionid=([^\\,]+)\\,action=([^\\,]+)\\,proto=([^\\,]+)\\,service=([^\\,]+)\\,attack=\\\"([^\\,]+)\\\"\\,srcport=([^\\,]+)\\,dstport=([^\\,]+)\\,direction=([^\\,]+)\\,attackid=([^\\,]+)\\,profile=\\\"([^\\,]+)\\\"\\,ref=\\\"([^\\,]+)\\\"\\,incidentserialno=([^\\,]+)\\,msg=\\\"([^\"]+)\\\",crscore=(\\d+)\\,crlevel=(\\S+)";
    final Pattern pattern_5 = Pattern.compile(logREX_5);

    private IPSClearner() {
    }

    public static IPSClearner getIPSClearner() {
        if (instance == null) {
            synchronized (IPSClearner.class) {
                if (instance == null)
                    instance = new IPSClearner();
            }
        }

        return instance;
    }

    public boolean isCompleted(String log) {

        Matcher matcher_1 = pattern_1.matcher(log);
        if (matcher_1.find()) {
            return true;
        }

        Matcher matcher_2 = pattern_2.matcher(log);
        if (matcher_2.find()) {
            return true;
        }

        Matcher matcher_3 = pattern_3.matcher(log);
        if (matcher_3.find()) {
            return true;
        }

        Matcher matcher_4 = pattern_4.matcher(log);
        if (matcher_4.find()) {
            return true;
        }

        Matcher matcher_5 = pattern_5.matcher(log);
        if (matcher_5.find()) {
            return true;
        }

        return false;
    }

}
