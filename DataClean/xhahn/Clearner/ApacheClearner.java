package com.xhahn.Clearner;

import com.xhahn.JavaBean.ApacheLog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: xhahn
 * Data: 2017/1/16/0016
 * Time: 22:11
 */
public class ApacheClearner {

    private static volatile ApacheClearner instance;

    private ApacheClearner() {
    }

    public static ApacheClearner getApacheClearner() {
        if (instance == null) {
            synchronized (ApacheClearner.class) {
                if (instance == null)
                    instance = new ApacheClearner();
            }
        }

        return instance;
    }

    public boolean isCompleted(String log) {
        final String logREX = "^(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s\\-\\s\\-\\s(\\[\\d+\\/\\S+\\/\\d{4}\\:\\d{2}\\:\\d{2}\\:\\d{2}\\s\\+\\d{4}\\])\\s\\\"(\\S+)\\s(\\S+\\s\\S+)\\\"\\s(\\S+)\\s(\\S+)";
        final Pattern pattern = Pattern.compile(logREX);
        Matcher matcher = pattern.matcher(log);
        if (!matcher.find()) {
            return false;
        }

        return true;
    }


    /**
     * 待清理日志
     * 文件类型为：jpg gif ico
     * http方法为：post
     * 状态码开头为：4或5
     *
     * @param log
     * @return
     */
    public boolean isValid(ApacheLog log) {

        boolean result = true;

        if (log.getHttpMethod().toLowerCase().equals("post"))
            result = false;

        else if (log.getStatue() >= 400)
            result = false;

        else {
            String url = log.getUrl().split(" ")[0];
            String type = url.substring(url.lastIndexOf(".") + 1);
            if (type.equals("jps") || type.equals("gif") || type.equals("ico"))
                result = false;

        }

        return result;
    }
}
