package com.bilin.job;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.bilin.core.MapOutValue;
import com.bilin.core.RunReduce;
import com.bilin.main.Config;

public class LogProcessReducer extends Reducer<Text, MapOutValue, Text, Text> {

	private RunReduce runReduce;
	private MultipleOutputs<Text, Text> mos;
	private String logType;
	private Text result = new Text();
	
	private final String SEPERATOR = ".";
	
	public void loadConfig(Context context){
		logType = context.getConfiguration().get("logType");
		String filePath = context.getConfiguration().get("property_file_path");
		Config.getInstance().loadConfig(logType, filePath);
	}

	@Override
	protected void setup(Context context) {
		this.loadConfig(context);
		
		mos = new MultipleOutputs<Text, Text>(context);
		String clsName = Config.getInstance().getReduceClsName();
		try {
			runReduce = (RunReduce) Class.forName(clsName).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(Text key, Iterable<MapOutValue> mapOutValue,
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
		String fileName = this.logType + this.SEPERATOR + key.substring(timeIndex + 1);
		return fileName;
	}
	
}
