package job;

import java.io.IOException;
import java.util.Map;

import main.Config;
import mapred.RunMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProcessMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	RunMapper runner;
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		Map<Text,LongWritable> result = runner.run(value, Config.getInstance().getLogFormat());
		if(result == null){
			context.getCounter("Error_log", "shorter_then_require").increment(1);
		}else{
			for(Text keyy : result.keySet()){
				if(keyy.toString().startsWith("Error")){
					context.getCounter("Error_log", keyy.toString()).increment(result.get(keyy).get());
				}else
					context.write(keyy, result.get(keyy));
			}
			result.clear();
		}
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String filename = context.getConfiguration().get("CONFPATH");
		try {
			Config.getInstance().loadConfig(filename);
			String ClassName = Config.getInstance().getMapperClass();
			runner = (RunMapper) Class.forName(ClassName).newInstance();
//			if(ClassName.equals("mapred.CNMapper")){
//				URI[] localCacheFile = context.getCacheFiles();                //get ip to geo code file china.csv and loading
//		        FileSystem fs = FileSystem.get(localCacheFile[0], new Configuration());
//		        IpToGeo.loadGeoFile(localCacheFile[0].toString(),fs);
//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
