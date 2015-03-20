package com.bilin.main;

import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class Config {

    public static Map<String, Integer> logFormat = new HashMap<String, Integer>();

    public static Map<String, String> dimensionsMap = new HashMap<String, String>();

    public static Map<Integer, String> freqFormat = new HashMap<Integer, String>();

    public static String mapClsName;

    public static String reduceClsName;

    public static final String COMMA = ",";

    public static String logType;

    public static boolean isCPM = false;

    public static int cpmPos;

    public static int user_id_pos;

    public static final String MAP_CLS = ".mapCls";

    public static final String REDUCE_CLS = ".reduceCls";

    public static final String DIMENSION = "dimension.";

    public static final String CPM = ".cpm";

    public static final String WIN = "win";

    public static Log log = LogFactory.getLog("rootLogger");

    public static final String TIMELAG = "timelag";

    //time lag v1.0.0
//    public boolean time_lag = false;
//    public float timelag_value = Float.MAX_VALUE;

    //time lag v1.0.1
    public Map<String,Double> timeLag = new HashMap<String, Double>();

    public static Config config = null;

    static {
        if (config == null) {
            config = new Config();
        }
    }

    public static Config getInstance() {
        if (config == null) {
            config = new Config();
        }
        return config;
    }

    public void loadConfig(String logType, String filePath) {
        Config.logType = logType;

        FSDataInputStream in;
        Properties props = new Properties();

        Configuration conf = new Configuration();
        try {
            //use this function to get fileSystem for using the s3 service
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            in = fs.open(new Path(filePath));
            try {
                props.load(in);
                StringTokenizer logformat = new StringTokenizer(props.getProperty(logType), Config.COMMA);
                int pos = 0;
                while (logformat.hasMoreTokens()) {
                    logFormat.put(logformat.nextToken(), pos);
                    pos++;
                }
                if (logType.equals("req")) {
                    StringTokenizer freqformat = new StringTokenizer(props.getProperty("freq"), Config.COMMA);
                    while (freqformat.hasMoreTokens()) {
                        String at = freqformat.nextToken();
                        freqFormat.put(logFormat.get(at), at);
                    }
                }
                Config.mapClsName = props.getProperty(logType + Config.MAP_CLS);
                Config.reduceClsName = props.getProperty(logType + Config.REDUCE_CLS);
                Enumeration<?> en = props.propertyNames();
                while (en.hasMoreElements()) {
                    String key = (String) en.nextElement();
                    if (key.contains(Config.DIMENSION))
                        Config.dimensionsMap.put(key, props.getProperty(key));
                }
                String[] mapOutCpm = props.getProperty(logType + Config.CPM).split(Config.COMMA);
                Config.isCPM = Boolean.parseBoolean(mapOutCpm[0]);
                if (Config.isCPM) {
                    Config.cpmPos = Config.logFormat.get(mapOutCpm[1]);
                }

                if (isWinLog()) {
                    Config.user_id_pos = Config.logFormat.get(mapOutCpm[2]);
                }
                timeLag.clear();
                StringTokenizer st = new StringTokenizer(props.getProperty(TIMELAG), COMMA);
                while(st.hasMoreTokens()){
                    String time_lag = st.nextToken();
                    String[] tl = time_lag.split(":");
                    timeLag.put(tl[0],Double.parseDouble(tl[1]));
                }
//                time_lag = Boolean.parseBoolean(timeLag[0]);
//                if (time_lag)
//                    timelag_value = Float.parseFloat(timeLag[1]);
            } finally {
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * Get log fields from the configuration file
     */
    public void loadLogFields(String logType, String filePath) throws IOException {
        Config.logType = logType;

        Properties props = new Properties();
        Configuration conf = new Configuration();

        //use this function to get fileSystem for using the s3 service
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);

        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(filePath));

            props.load(in);
            StringTokenizer logformat = new StringTokenizer(props.getProperty(logType), Config.COMMA);
            int pos = 0;
            while (logformat.hasMoreTokens()) {
                logFormat.put(logformat.nextToken(), pos);
                pos++;
            }

        } finally {
            if (in != null)
                in.close();
        }
    }

    public boolean isWinLog() {
        if (0 == Config.logType.compareTo(WIN))
            return true;
        else
            return false;
    }

    public Map<String, Integer> getLogFormatMap() {
        return Config.logFormat;
    }

    public Map<String, String> getDimensionMap() {
        return Config.dimensionsMap;
    }

    public String getMapClassName() {
        return Config.mapClsName;
    }

    public String getReduceClsName() {
        return Config.reduceClsName;
    }

    public Map<Integer, String> getFreqFormat() {
        return Config.freqFormat;
    }

//    public float getTimelag_value() {
//        return timelag_value;
//    }

//    public boolean isTime_lag() {
//        return time_lag;
//    }

    public Map<String, Double> getTimeLag() {
        return timeLag;
    }
}
