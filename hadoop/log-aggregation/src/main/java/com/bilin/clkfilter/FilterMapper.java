package com.bilin.clkfilter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bilin.main.Config;

public class FilterMapper extends Mapper<LongWritable, Text, FilterKey, Text> {
	
	public static final String PROPERTY_FILE_PATH = "property_file_path";
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Config.getInstance().loadLogFields("clk", context.getConfiguration()
                .get(PROPERTY_FILE_PATH));
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] logSplits = value.toString().split("\t");
		FilterKey filterKey = new FilterKey();
		filterKey.set(logSplits[Config.logFormat.get("user_ip")],
				logSplits[Config.logFormat.get("domain")],
				Long.parseLong(logSplits[Config.logFormat.get("time")]));
		
		context.write(filterKey, value);
	}
}
