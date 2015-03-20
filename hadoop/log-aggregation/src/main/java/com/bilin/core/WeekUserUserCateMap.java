package com.bilin.core;

import com.bilin.main.WeekUserConfig;
import com.bilin.utils.WeekUserMapTools;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WeekUserUserCateMap implements WeekUserRunMap {

    public Map<Text, WeekUserMapOutValue> result = new HashMap<Text, WeekUserMapOutValue>();

    public ArrayList<String> splits;

    private WeekUserMapTools weekUserMapTools = new WeekUserMapTools();

    private String delim = ",";

    @Override
    public Map<Text, WeekUserMapOutValue> run(Text lineOfUserCategory) {
        result.clear();
        splits = weekUserMapTools.split(lineOfUserCategory.toString(), delim);
        if (WeekUserConfig.weekUserConfig.getLogFormatMap().size() != splits.size()) {
            return null;
        }
        for (String str : WeekUserConfig.weekUserConfig.getDimensionMap().keySet()) {
            result.put(weekUserMapTools.buildKey(str, splits), weekUserMapTools.buildValue(WeekUserConfig.weekUserConfig.dimension_mapout_val,
                    splits));
        }
        return result;
    }
}
