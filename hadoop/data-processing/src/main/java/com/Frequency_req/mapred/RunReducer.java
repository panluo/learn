package mapred;

import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;

public interface RunReducer {
	public LongWritable run(Iterator<LongWritable> value);
}
