package com.bilin.core;

import org.apache.hadoop.io.Text;

import java.util.Map;

public interface UserCateRunMap {

    public Map<Text, UserCateMapOutValue> run(Text lineOfLog);
}
