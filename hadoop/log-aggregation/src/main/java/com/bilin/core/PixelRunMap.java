package com.bilin.core;

import org.apache.hadoop.io.Text;

import java.util.Map;

public interface PixelRunMap {

    public Map<Text, Text> run(Text lineOfLog);
}
