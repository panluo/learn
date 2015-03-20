package com.bilin.core;

import com.bilin.main.Config;
import com.bilin.utils.MapTools;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CPMMap implements RunMap {

    public static final int ONE = 1;

    public MapOutValue mapOutVal = null;

    public Map<Text, MapOutValue> result = new HashMap<Text, MapOutValue>();

    public ArrayList<String> splits;

    MapTools mapTools = new MapTools();

    @Override
    public Map<Text, MapOutValue> run(Text lineOfLog) {
        result.clear();
        splits = mapTools.split(lineOfLog.toString(), "\t");
        if (splits.size() != Config.logFormat.size()){
            System.out.println(lineOfLog.toString());
            return null;
        }
        for (String dimension : Config.getInstance().getDimensionMap().keySet()) {
            double cpm = 0;
            if (!"".equals(splits.get(Config.cpmPos)) && !" ".equals(splits.get(Config.cpmPos)) || !"nil".equals
                    (splits.get(Config.cpmPos)))
                cpm = Double.parseDouble(splits.get(Config.cpmPos));
            mapOutVal = new MapOutValue(ONE, cpm);
            result.put(mapTools.buildKey(dimension, splits), mapOutVal);
        }
        return result;
    }
}
