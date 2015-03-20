package com.bilin.core;

import com.bilin.main.UsesrCateConfig;
import com.bilin.utils.UserCateMapTools;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UserCateMap implements UserCateRunMap {

    public Map<Text, UserCateMapOutValue> result = new HashMap<Text, UserCateMapOutValue>();

    public ArrayList<String> splits;

    UserCateMapTools userCateMapTools = new UserCateMapTools();

    @Override
    public Map<Text, UserCateMapOutValue> run(Text lineOfLog) {
        result.clear();
        splits = userCateMapTools.split(lineOfLog.toString(), "\t");
        if (UsesrCateConfig.usesrCateConfig.getLogFormatMap().size() != splits.size()) {
            return null;
        }
        for (String str : UsesrCateConfig.usesrCateConfig.getDimensionMap().keySet()) {
            result.put(userCateMapTools.buildKey(str, splits), userCateMapTools.buildValue(UsesrCateConfig.usesrCateConfig.dimension_mapout_val,
                    splits));
        }
        return result;
    }
}
