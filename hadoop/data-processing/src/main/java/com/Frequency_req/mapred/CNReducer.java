package mapred;

import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;

public class CNReducer extends USReducer {

	@Override
	public LongWritable run(Iterator<LongWritable> logformat) {
		return super.run(logformat);
	}
	
}
