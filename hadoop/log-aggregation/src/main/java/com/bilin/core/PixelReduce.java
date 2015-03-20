package com.bilin.core;

import org.apache.hadoop.io.Text;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class PixelReduce implements PixelRunReduce {

    private static long count;
    private Iterator<Text> values;
    private static final String SEPERATOR = "\t";

    @Override
    public void setValue(Iterable<Text> mapOutValue) {
        values = mapOutValue.iterator();
    }

    public void init() {
        count = 0;
    }

    @Override
    public String run() {
        this.init();
        Set<Text> userIdSet = new HashSet<Text>();
        while (values.hasNext()) {
            userIdSet.add(values.next());
            count ++;
        }
        return String.valueOf(count) + SEPERATOR + userIdSet.size();
    }
}
