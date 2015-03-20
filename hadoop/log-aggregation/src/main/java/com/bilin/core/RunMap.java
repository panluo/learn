package com.bilin.core;

import org.apache.hadoop.io.Text;

import java.util.Map;

public interface RunMap {

    public Map<Text, MapOutValue> run(Text lineOfLog);
}
