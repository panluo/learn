package com.bilin.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

public class PixelConfig {

    public static Map<String, Integer> logFormat = new HashMap<String, Integer>();

    public static Map<String, String> dimensionsMap = new HashMap<String, String>();

    public static String mapClsName;

    public static String reduceClsName;

    public static final String COMMA = ",";

    public static String logType;

    public static final String MAP_CLS = ".mapCls";

    public static final String REDUCE_CLS = ".reduceCls";

    public static final String DIMENSION = "dimension.";

    public static int user_id_pos;

//    public static Log log = LogFactory.getLog("rootLogger");

    public static PixelConfig pixelConfig = null;

    static {
        if (pixelConfig == null) {
            pixelConfig = new PixelConfig();
        }
    }

    public static PixelConfig getInstance() {
        if (pixelConfig == null) {
            pixelConfig = new PixelConfig();
        }
        return pixelConfig;
    }

    public void loadConfig(String logType, String filePath) {
        PixelConfig.logType = logType;

        InputStream in;
        Properties props = new Properties();

        Configuration conf = new Configuration();
        try {
            //use this function to get fileSystem for using the s3 service
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            in = fs.open(new Path(filePath));
            try {
                props.load(in);
                StringTokenizer logformat = new StringTokenizer(props.getProperty(logType), PixelConfig.COMMA);
                int pos = 0;
                while (logformat.hasMoreTokens()) {
                    logFormat.put(logformat.nextToken(), pos);
                    pos++;
                }
                PixelConfig.mapClsName = props.getProperty(logType + PixelConfig.MAP_CLS);
                PixelConfig.reduceClsName = props.getProperty(logType + PixelConfig.REDUCE_CLS);
                Enumeration<?> en = props.propertyNames();
                while (en.hasMoreElements()) {
                    String key = (String) en.nextElement();
                    if (key.contains(PixelConfig.DIMENSION))
                        PixelConfig.dimensionsMap.put(key, props.getProperty(key));
                }
                PixelConfig.user_id_pos = PixelConfig.logFormat.get(props.getProperty(logType + ".userid"));
            } finally {
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Integer> getLogFormatMap() {
        return PixelConfig.logFormat;
    }

    public Map<String, String> getDimensionMap() {
        return PixelConfig.dimensionsMap;
    }

    public String getMapClassName() {
        return PixelConfig.mapClsName;
    }

    public String getReduceClsName() {
        return PixelConfig.reduceClsName;
    }
}
