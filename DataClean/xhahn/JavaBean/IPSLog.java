package com.xhahn.JavaBean;

import java.io.Serializable;

/**
 * User: xhahn
 * Data: 2017/1/11/0011
 * Time: 22:25
 */
public class IPSLog implements Serializable{
    private String date;
    private String time;
    private Integer logid;
    private String type;
    private String subtype;
    private String level;
    private String vd;
    private String srcip;
    private Integer srcport;
    private String dstip;
    private Integer dstport;
    private String sessionid;
    private Integer proto;
    private String action;

    public String getDate() {
        return date;
    }

    public IPSLog setDate(String date) {
        this.date = date;
        return this;
    }

    public String getTime() {
        return time;
    }

    public IPSLog setTime(String time) {
        this.time = time;
        return this;
    }


    public Integer getLogid() {
        return logid;
    }

    public IPSLog setLogid(Integer logid) {
        this.logid = logid;
        return this;
    }

    public String getType() {
        return type;
    }

    public IPSLog setType(String type) {
        this.type = type;
        return this;
    }

    public String getSubtype() {
        return subtype;
    }

    public IPSLog setSubtype(String subtype) {
        this.subtype = subtype;
        return this;
    }

    public String getLevel() {
        return level;
    }

    public IPSLog setLevel(String level) {
        this.level = level;
        return this;
    }

    public String getVd() {
        return vd;
    }

    public IPSLog setVd(String vd) {
        this.vd = vd;
        return this;
    }

    public String getSrcip() {
        return srcip;
    }

    public IPSLog setSrcip(String srcip) {
        this.srcip = srcip;
        return this;
    }

    public Integer getSrcport() {
        return srcport;
    }

    public IPSLog setSrcport(Integer srcport) {
        this.srcport = srcport;
        return this;
    }

    public String getDstip() {
        return dstip;
    }

    public IPSLog setDstip(String dstip) {
        this.dstip = dstip;
        return this;
    }

    public Integer getDstport() {
        return dstport;
    }

    public IPSLog setDstport(Integer dstport) {
        this.dstport = dstport;
        return this;
    }


    public String getSessionid() {
        return sessionid;
    }

    public IPSLog setSessionid(String sessionid) {
        this.sessionid = sessionid;
        return this;
    }


    public Integer getProto() {
        return proto;
    }

    public IPSLog setProto(Integer proto) {
        this.proto = proto;
        return this;
    }

    public String getAction() {
        return action;
    }

    public IPSLog setAction(String action) {
        this.action = action;
        return this;
    }


    @Override
    public String toString() {
        return action +
                " 1:" + date +
                " 2:" + time +
                " 3:" + logid +
                " 4:" + type +
                " 5:" + subtype +
                " 6:" + level +
                " 7:" + vd +
                " 8:" + srcip +
                " 9:" + srcport +
                " 10:" + dstip +
                " 11:" + dstport +
                " 12:" + sessionid +
                " 13:" + proto;
    }
}
