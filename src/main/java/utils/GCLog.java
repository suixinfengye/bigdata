package utils;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 用于找出GC时间过长的记录
 * feng
 * 18-12-9
 */
public class GCLog {
    public static void main(String args[]) {
        GCLog gcLog = new GCLog();
        gcLog.readFileByLines("/home/feng/file/ip101/logs/worker_gc.log");
        gcLog.readFileByLines("/home/feng/file/ip101/logs/master_gc.log");
        gcLog.readFileByLines("/home/feng/file/ip101/logs/executor_gc.log");
        gcLog.readFileByLines("/home/feng/file/ip107/logs/worker_gc.log");
        gcLog.readFileByLines("/home/feng/file/ip107/logs/executor_gc.log");
    }

    public void readFileByLines(String filePath) {
        System.out.println("path:"+filePath);
        File file = new File(filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String temp = null;
            int line = 1;
            while ((temp = reader.readLine()) != null) {
                match(temp, line++);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println();
    }

    public void match(String content, int line) {
        String regEx = "user=\\d+\\.\\d+ sys=\\d+\\.\\d+, real=\\d+\\.\\d+";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(content);
        while (m.find()) {
            String result = m.group();
            GCInfo gcInfo = new GCInfo(result, line);
            System.out.println(gcInfo);
        }
    }

}

class GCInfo {
    private int line;
    private double user;
    private double sys;
    private double real;
    private static final double GC_VALUE = 0.5d;
    private static final String FLAG_LINE = "------------------------";

    public GCInfo(String gcContent, int line) {
        this.line = line;
        setGCProperties(gcContent);
    }

    public void setGCProperties(String gcContent) {
        String[] strs = gcContent.split(" ");
        this.user = Double.valueOf(strs[0].substring(5));
        this.sys = Double.valueOf(strs[1].substring(4, strs[1].length() - 1));
        this.real = Double.valueOf(strs[2].substring(5));
    }

    private boolean checkGood() {
        boolean isGood = true;
        if (user > GC_VALUE || sys > GC_VALUE || real > GC_VALUE) {
            isGood = false;
        }
        return isGood;
    }

    @Override
    public String toString() {
        String str = "GCInfo{" +
                "line=" + line +
                ", user=" + user +
                ", sys=" + sys +
                ", real=" + real +
                '}';
        if (!checkGood()) {
            str = str + FLAG_LINE;
        }
        return str;
    }
}