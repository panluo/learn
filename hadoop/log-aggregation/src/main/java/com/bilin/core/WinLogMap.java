package com.bilin.core;

import com.bilin.main.Config;
import com.bilin.utils.MapTools;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WinLogMap implements RunMap {

    public static final int ONE = 1;

    public MapOutValue mapOutVal = null;

    public Map<Text, MapOutValue> result = new HashMap<Text, MapOutValue>();

    public ArrayList<String> splits;

    public MapTools mapTools = new MapTools();

    public float bidReqTime = 0, winReqTime = 0;

    @Override
    public Map<Text, MapOutValue> run(Text lineOfLog) {
        result.clear();
        splits = mapTools.split(lineOfLog.toString(), "\t");
        if (splits.size() != Config.logFormat.size()) {
            System.out.println(lineOfLog.toString());
            return null;
        }
        double timeLag = Config.config.getTimeLag().get(splits.get(Config.logFormat.get("ad_exchange")));
        if (0 < timeLag) {
            bidReqTime = Float.parseFloat(splits.get(Config.logFormat.get("bid_req_start_time")));
            winReqTime = Float.parseFloat(splits.get(Config.logFormat.get("win_response_time")));
            if (timeLag < winReqTime - bidReqTime)
                return null;
        }
        for (String dimension : Config.getInstance().getDimensionMap().keySet()) {
            double cpm = 0;
            String user_id = "unknown";
            if (!"".equals(splits.get(Config.cpmPos)) && !" ".equals(splits.get(Config.cpmPos)) && !"nil".equals
                    (splits.get(Config.cpmPos)))
                cpm = Double.parseDouble(splits.get(Config.cpmPos));
            if (!"".equals(splits.get(Config.user_id_pos)) && !" ".equals(splits.get(Config.user_id_pos)) && !"nil"
                    .equals(splits.get(Config.user_id_pos)))
                user_id = splits.get(Config.user_id_pos);
            mapOutVal = new MapOutValue(ONE, cpm, user_id);
            result.put(mapTools.buildKey(dimension, splits), mapOutVal);
        }
        return result;
    }
}
