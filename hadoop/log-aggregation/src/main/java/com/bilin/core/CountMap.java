package com.bilin.core;

import com.bilin.main.Config;
import com.bilin.utils.MapTools;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CountMap implements RunMap {

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
            mapOutVal = new MapOutValue(ONE);
            result.put(mapTools.buildKey(dimension, splits), mapOutVal);
        }
        return result;
    }
}
