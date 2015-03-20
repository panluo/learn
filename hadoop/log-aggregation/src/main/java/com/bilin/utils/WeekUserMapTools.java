package com.bilin.utils;

import com.bilin.core.WeekUserMapOutValue;
import com.bilin.main.WeekUserConfig;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class WeekUserMapTools {

    public StringBuffer strbuf = new StringBuffer();

    public String[] dimension;

    public static final String COMMA = ",";

    public ArrayList<String> splits = new ArrayList<String>();

    public Text buildKey(String str, ArrayList<String> split) {
        dimension = WeekUserConfig.weekUserConfig.getDimensionMap().get(str).split(COMMA);
        strbuf.setLength(0);
        for (int i = 0; i < dimension.length; i++) {
            strbuf.append(split.get(WeekUserConfig.weekUserConfig.getLogFormatMap().get(dimension[i])));
            strbuf.append(COMMA);
        }
        return new Text(strbuf.substring(0, strbuf.length() - 1));
    }

    public WeekUserMapOutValue buildValue(String str, ArrayList<String> split) {
        String[] dimension_value = str.split(COMMA);
        String site_category = split.get(WeekUserConfig.weekUserConfig.logFormat.get(dimension_value[0]));
        String page_category = split.get(WeekUserConfig.weekUserConfig.logFormat.get(dimension_value[1]));
        String max_time_stamp = split.get(WeekUserConfig.weekUserConfig.logFormat.get(dimension_value[2]));
        String min_time_stamp = split.get(WeekUserConfig.weekUserConfig.logFormat.get(dimension_value[3]));
        String total = split.get(WeekUserConfig.weekUserConfig.logFormat.get(dimension_value[4]));
        return new WeekUserMapOutValue(site_category, page_category, max_time_stamp, min_time_stamp, total);
    }

    public ArrayList<String> split(String str, String delim) {
        splits.clear();
        int posBeg = 0, length = str.length(), posEnd = str.indexOf(delim, posBeg);
        while (length != posEnd && -1 != posEnd) {
            splits.add(str.substring(posBeg, posEnd));
            posBeg = posEnd + delim.length();
            posEnd = str.indexOf(delim, posBeg);
        }
        splits.add(str.substring(posBeg, length));
        return splits;
    }
}
