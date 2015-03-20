package com.bilin.utils;

import com.bilin.main.PixelConfig;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class PixelMapTools {

    public StringBuffer strbuff = new StringBuffer();

    public String[] dimension;

    public static final String COMMA = ",";

    public static final String POINT = ".";

    public static final String UNKNOWN = "unknown";

    public static final String DELIM = "|";

    public ArrayList<String> splits = new ArrayList<String>();

    public Text buildKey(String str, ArrayList<String> splits) {
        dimension = PixelConfig.pixelConfig.getDimensionMap().get(str).split(COMMA);
        strbuff.setLength(0);
        strbuff.append(str.substring(str.indexOf(".") + 1));
        strbuff.append(DELIM);
        int length = dimension.length;
        for (int i = 0; i < length - 1; i++) {
            if (isNull(splits.get(PixelConfig.pixelConfig.getLogFormatMap().get(dimension[i])))) {
                strbuff.append(splits.get(PixelConfig.pixelConfig.getLogFormatMap().get(dimension[i])));
            } else
                strbuff.append(UNKNOWN);
            strbuff.append(DELIM);
        }
        strbuff.append(Long.parseLong(splits.get(PixelConfig.pixelConfig.getLogFormatMap().get(dimension[length - 1]))) / 3600);
        return new Text(strbuff.toString());
    }

    public boolean isNull(String str) {
        return !"".equals(str) && !" ".equals(str) && !"nil".equals(str);
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