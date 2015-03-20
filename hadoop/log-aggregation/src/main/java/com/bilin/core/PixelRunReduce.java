package com.bilin.core;

import org.apache.hadoop.io.Text;

public interface PixelRunReduce {
	public void setValue(Iterable<Text> mapOutValue);
	public String run();
}
