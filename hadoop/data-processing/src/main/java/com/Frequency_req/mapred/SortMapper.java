package mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.Spliter;

public class SortMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		ArrayList<String> line = Spliter.spliter(value.toString(), "\t");
		if(line.size() != 2){
			context.getCounter("Error_In_Line", "splite_error").increment(1);
			System.out.println(value.toString());
			return;
		}
		context.write(value, NullWritable.get());
	}
	
}
