package com.xhahn.JavaBean;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * User: xhahn
 * Data: 2017/1/16/0016
 * Time: 21:19
 */
public class ApacheLog implements Serializable{
    private String ip;
    private Timestamp date;
    private String url;
    private Integer statue;
    private Integer data;
    private String httpMethod;
    private String httpProtocol;

    public String getHttpProtocol() {
        return httpProtocol;
    }

    public ApacheLog setHttpProtocol(String httpProtocol) {
        this.httpProtocol = httpProtocol;
        return this;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public ApacheLog setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public ApacheLog setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public Timestamp getDate() {
        return date;
    }

    public ApacheLog setDate(Timestamp date) {
        this.date = date;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public ApacheLog setUrl(String url) {
        this.url = url;
        return this;
    }

    public Integer getStatue() {
        return statue;
    }

    public ApacheLog setStatue(int statue) {
        this.statue = statue;
        return this;
    }

    public Integer getData() {
        return data;
    }

    public ApacheLog setData(int data) {
        this.data = data;
        return this;
    }
}
