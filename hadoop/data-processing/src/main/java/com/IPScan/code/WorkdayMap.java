package code;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WorkdayMap extends Mapper<LongWritable, Text, Text, MapOutValue> {

	Map<String,MapOutValue> result = null;
	int limit = 10;
	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, MapOutValue>.Context context)
			throws IOException, InterruptedException {
		String li = context.getConfiguration().get("limit");
		if(li!=null)
			limit = Integer.parseInt(li);
	}


	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, MapOutValue>.Context context)
			throws IOException, InterruptedException {
		result = RunMap.run(value.toString(), false, limit);
		if(result==null){
			context.getCounter("Error_Log", "miss_elements").increment(1);
		}else{
			for(String ip : result.keySet())
				context.write(new Text(ip), result.get(ip));
		}
	}
}
