package com.bilin.core;

import org.apache.hadoop.io.Text;

import java.util.Map;

public interface WeekUserRunMap {

//    public Text buildKey(String dimensionName);

    public Map<Text, WeekUserMapOutValue> run(Text lineOfLog);
}
