package mapred;

import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;

public class USReducer implements RunReducer {

	@Override
	public LongWritable run(Iterator<LongWritable> logformat) {
		long total = 0;
		while(logformat.hasNext()){
			total += logformat.next().get();
		}
		return new LongWritable(total);
	}

}
