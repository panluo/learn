package com.bilin.utils;

import com.bilin.core.UserCateMapOutValue;
import com.bilin.main.UsesrCateConfig;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class UserCateMapTools {

    public StringBuffer strbuf = new StringBuffer();

    public String[] dimension;

    public static final String COMMA = ",";

    public static final String UNKNOWN = "unknown";

//    public static final String DELIM = "_";

    public ArrayList<String> splits = new ArrayList<String>();

    public Text buildKey(String str, ArrayList<String> split) {
        dimension = UsesrCateConfig.usesrCateConfig.getDimensionMap().get(str).split(COMMA);
        strbuf.setLength(0);
        for (int i = 0; i < dimension.length; i++) {
            String tmp = split.get(UsesrCateConfig.usesrCateConfig.getLogFormatMap().get(dimension[i]));
            if ("".equals(tmp) || " ".equals(tmp))
                strbuf.append(UNKNOWN);
            else
                strbuf.append(tmp);
            strbuf.append(COMMA);
        }
        return new Text(strbuf.substring(0, strbuf.length() - 1));
    }

    public UserCateMapOutValue buildValue(String str, ArrayList<String> split) {
        String[] dimension_value = str.split(COMMA);
        String site_category = split.get(UsesrCateConfig.usesrCateConfig.logFormat.get(dimension_value[0]));
        if (site_category.equals(" ") || site_category.equals(""))
            site_category = UNKNOWN;
        String page_category = split.get(UsesrCateConfig.usesrCateConfig.logFormat.get(dimension_value[1]));
        if (page_category.equals(" ") || page_category.equals(""))
            page_category = UNKNOWN;
        String time_stamp = split.get(UsesrCateConfig.usesrCateConfig.logFormat.get(dimension_value[2]));
        return new UserCateMapOutValue(site_category, page_category, time_stamp);
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
