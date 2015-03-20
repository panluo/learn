package com.bilin.job;

//import com.bilin.core.PixelMapOutValue;
import com.bilin.core.PixelRunReduce;
import com.bilin.main.PixelConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class PixelLogProcessReducer extends Reducer<Text, Text, Text, Text> {

	private PixelRunReduce runReduce;
	private MultipleOutputs<Text, Text> mos;
	private String logType;
	private Text result = new Text();

    public void loadConfig(Context context){
		logType = context.getConfiguration().get("logType");
		String filePath = context.getConfiguration().get("property_file_path");
		PixelConfig.getInstance().loadConfig(logType, filePath);
	}

	@Override
	protected void setup(Context context) {
		this.loadConfig(context);
		
		mos = new MultipleOutputs<Text, Text>(context);
		String clsName = PixelConfig.getInstance().getReduceClsName();
		try {
			runReduce = (PixelRunReduce) Class.forName(clsName).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(Text key, Iterable<Text> mapOutValue,
			Context context) throws IOException, InterruptedException {
		runReduce.setValue(mapOutValue);
		result.set(runReduce.run());
		mos.write(key, result,this.getFileName(key.toString()));
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
	
	public String getFileName(String key){
		int timeIndex = key.lastIndexOf("|");
        String seperator = "-";
        return this.logType + seperator + key.substring(timeIndex + 1);
	}
	
}
