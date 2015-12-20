package com.xhahn.gmm.LogAnalyzer;


import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: xhahn
 * Data: 2015/8/26/0026
 * Time: 11:43
 */
public class AccessLogFile implements Serializable {
    private static final Logger logger = Logger.getLogger("Check");

    private String ONLINE_DETAIL_UUID;
    private String USER_ID;
    private String USER_MAC;
    private String USER_IPV4;
    private String LOGIN_TIME;
    private String LOGOUT_TIME;
    private String ONLINE_SEC;
    private String ACCESS_TYPE;
    private String USERTEMPLATE_ID;
    private String PACKAGE_NAME;
    private String SERVICE_SUFFIX;

    public AccessLogFile(String ONLINE_DETAIL_UUID, String USER_ID, String USER_MAC,
                         String USER_IPV4, String LOGIN_TIME, String LOGOUT_TIME,
                         String ONLINE_SEC, String ACCESS_TYPE, String SERVICE_SUFFIX) {
        this.ONLINE_DETAIL_UUID = ONLINE_DETAIL_UUID;
        this.USER_ID = USER_ID;
        this.USER_MAC = USER_MAC;
        this.USER_IPV4 = USER_IPV4;
        this.LOGIN_TIME = LOGIN_TIME;
        this.LOGOUT_TIME = LOGOUT_TIME;
        this.ONLINE_SEC = ONLINE_SEC;
        this.ACCESS_TYPE = ACCESS_TYPE;
        this.SERVICE_SUFFIX = SERVICE_SUFFIX;

        // this.USERTEMPLATE_ID = USERTEMPLATE_ID;
        // this.PACKAGE_NAME = PACKAGE_NAME;
    }

    public String getSERVICE_SUFFIX() {
        return SERVICE_SUFFIX;
    }

    public void setSERVICE_SUFFIX(String SERVICE_SUFFIX) {
        this.SERVICE_SUFFIX = SERVICE_SUFFIX;
    }

    public String getPACKAGE_NAME() {
        return PACKAGE_NAME;
    }

    public void setPACKAGE_NAME(String PACKAGE_NAME) {
        this.PACKAGE_NAME = PACKAGE_NAME;
    }

    public String getUSERTEMPLATE_ID() {
        return USERTEMPLATE_ID;
    }

    public void setUSERTEMPLATE_ID(String USERTEMPLATE_ID) {
        this.USERTEMPLATE_ID = USERTEMPLATE_ID;
    }

    public String getACCESS_TYPE() {
        return ACCESS_TYPE;
    }

    public void setACCESS_TYPE(String ACCESS_TYPE) {
        this.ACCESS_TYPE = ACCESS_TYPE;
    }

    public String getONLINE_SEC() {
        return ONLINE_SEC;
    }

    public void setONLINE_SEC(String ONLINE_SEC) {
        this.ONLINE_SEC = ONLINE_SEC;
    }

    public String getLOGOUT_TIME() {
        return LOGOUT_TIME;
    }

    public void setLOGOUT_TIME(String LOGOUT_TIME) {
        this.LOGOUT_TIME = LOGOUT_TIME;
    }

    public String getLOGIN_TIME() {
        return LOGIN_TIME;
    }

    public void setLOGIN_TIME(String LOGIN_TIME) {
        this.LOGIN_TIME = LOGIN_TIME;
    }

    public String getUSER_IPV4() {
        return USER_IPV4;
    }

    public void setUSER_IPV4(String USER_IPV4) {
        this.USER_IPV4 = USER_IPV4;
    }

    public String getUSER_MAC() {
        return USER_MAC;
    }

    public void setUSER_MAC(String USER_MAC) {
        this.USER_MAC = USER_MAC;
    }

    public String getUSER_ID() {
        return USER_ID;
    }

    public void setUSER_ID(String USER_ID) {
        this.USER_ID = USER_ID;
    }

    public String getONLINE_DETAIL_UUID() {
        return ONLINE_DETAIL_UUID;
    }

    public void setONLINE_DETAIL_UUID(String ONLINE_DETAIL_UUID) {
        this.ONLINE_DETAIL_UUID = ONLINE_DETAIL_UUID;
    }


    //example of logline:
    //1:ONLINE_DETAIL_UUID 2:USER_ID 3:USER_MAC 4:USER_IPV4 5:LOGIN_TIME 6:LOGOUT_TIME 7:ONLINE_SEC 8:ACCESS_TYPE 9:USERTEMPLATE_ID 10:PACKAGE_NAME 11:SERVICE_SUFFIX
    //84848482430b1d87014349638479637e,U201114260,A4BADBCE6B49,0.0.0.0,2014-01-01 00:01:35.140000000,2014-01-01 00:01:35.140000000,0,1,��������̬IPģ��,20Ԫÿ��,internet
    private static final String LOGPATTERN =
            "^(\\S+),(\\S+),(\\S+),(\\S+),([\\S+]+\\s[\\d+\\:]+\\.\\d+),([\\S+]+\\s[\\d+\\:]+\\.\\d+),(\\d+),(\\d+),(\\S+),(\\S+),(\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOGPATTERN);

    public static AccessLogFile parseAccessLogFile(String logline) {
        Matcher m = PATTERN.matcher(logline);
//        System.out.println(m.find());
//        System.out.println(m.find());
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + logline);
            throw new RuntimeException("Error parsing logline"+logline);
        }
        //        System.out.println(m.group(1)+ " "+m.group(2)+" "+m.group(3)+" "+m.group(4)+" "+
//                m.group(5)+" "+m.group(6)+" "+m.group(7)+" "+m.group(8)+" "+m.group(11)+" "+m.group(9));


        return new AccessLogFile(m.group(1), m.group(2), m.group(3), m.group(4),
                m.group(5), m.group(6), m.group(7), m.group(8), m.group(11));
    }

}