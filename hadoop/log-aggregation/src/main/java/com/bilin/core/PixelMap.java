package com.bilin.core;

import com.bilin.main.PixelConfig;
import com.bilin.utils.PixelMapTools;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PixelMap implements PixelRunMap {

    private Map<Text, Text> result = new HashMap<Text, Text>();

    private PixelMapTools pixelMapTools = new PixelMapTools();

    @Override
    public Map<Text, Text> run(Text lineOfLog) {
//        splits.clear();
        result.clear();
        ArrayList<String> splits = pixelMapTools.split(lineOfLog.toString(), "\t");
        if (splits.size() < PixelConfig.pixelConfig.getLogFormatMap().size()) {
            System.out.println(lineOfLog.toString());
            return null;
        }
        for (String dim : PixelConfig.pixelConfig.getDimensionMap().keySet()) {
            result.put(pixelMapTools.buildKey(dim, splits), new Text(splits.get(PixelConfig.user_id_pos)));
        }
        return result;
    }
}
