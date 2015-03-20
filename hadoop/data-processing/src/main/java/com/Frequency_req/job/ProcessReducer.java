package job;

import java.io.IOException;

import main.Config;
import mapred.RunReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProcessReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	RunReducer runreducer;
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2)
			throws IOException, InterruptedException {
		LongWritable result = runreducer.run(arg1.iterator());
		arg2.write(arg0,result);
	}

	@Override
	protected void setup(
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String ClassName = Config.getInstance().getReducerClass();
	
		try {
			runreducer = (RunReducer) Class.forName(ClassName).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
