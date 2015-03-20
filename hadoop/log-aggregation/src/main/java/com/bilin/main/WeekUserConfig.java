package com.bilin.main;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class WeekUserConfig {

//    public static final String CONF_FILE = "hdfs://master.bilintechnology.net:8020/conf/bilin.properties";

    public Map<String, Integer> logFormat = new HashMap<String, Integer>();

    public Map<String, String> dimensions_Key_Map = new HashMap<String, String>();

//    public Map<String, String> dimensions_Val_Map = new HashMap<String, String>();

    public static String mapClsName;

    public static String reduceClsName;

    public static final String COMMA = ",";

//    public static String logType;

    public static final String MAP_CLS = ".mapCls";

    public static final String REDUCE_CLS = ".reduceCls";

    public static final String DIMENSION_KEY = "dimension.key";

    public static final String DIMENSION_MAPOUT_VAL = "dimension.value";

    public static String dimension_mapout_val;

    public static Log log = LogFactory.getLog("rootLogger");

    public static WeekUserConfig weekUserConfig = null;

    public static String PAGE_THRESHOLD = "page_threshold";

    public static String SITE_THRESHOLD = "site_threshold";

    public static float page_threshold = 0;

    public static float site_threshold = 0;

    static {
        if (weekUserConfig == null) {
            weekUserConfig = new WeekUserConfig();
        }
    }

    public static WeekUserConfig getInstance() {
        if (weekUserConfig == null) {
            weekUserConfig = new WeekUserConfig();
        }
        return weekUserConfig;
    }

    public void loadConfig(String logType, String filePath) {
        InputStream in;
        Properties props = new Properties();
        Configuration conf = new Configuration();
        logFormat.clear();
        dimensions_Key_Map.clear();
        try {
            FileSystem fs = FileSystem.get(conf);
            in = fs.open(new Path(filePath));
            try {
                props.load(in);
                StringTokenizer logformat = new StringTokenizer(props.getProperty(logType), WeekUserConfig.COMMA);
                int pos = 0;
                while (logformat.hasMoreTokens()) {
                    logFormat.put(logformat.nextToken(), pos);
                    pos++;
                }
                WeekUserConfig.mapClsName = props.getProperty(logType + WeekUserConfig.MAP_CLS);
                WeekUserConfig.reduceClsName = props.getProperty(logType + WeekUserConfig.REDUCE_CLS);
                Enumeration<?> en = props.propertyNames();
                while (en.hasMoreElements()) {
                    String key = (String) en.nextElement();
                    if (key.contains(WeekUserConfig.DIMENSION_KEY))
                        this.dimensions_Key_Map.put(key, props.getProperty(key));
                }
                dimension_mapout_val = props.getProperty(DIMENSION_MAPOUT_VAL);
                page_threshold = Float.parseFloat(props.getProperty(PAGE_THRESHOLD));
                site_threshold = Float.parseFloat(props.getProperty(SITE_THRESHOLD));
            } finally {
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Integer> getLogFormatMap() {
        return this.logFormat;
    }

    public Map<String, String> getDimensionMap() {
        return this.dimensions_Key_Map;
    }

    public String getMapClassName() {
        return WeekUserConfig.mapClsName;
    }

    public String getReduceClsName() {
        return WeekUserConfig.reduceClsName;
    }

    public static float getPage_threshold() {
        return page_threshold;
    }

    public static float getSite_threshold() {
        return site_threshold;
    }
}
