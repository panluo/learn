package com.bilin.clkfilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<FilterKey, Text, NullWritable, Text> {
	
	private int timeThreshold = 180;
	private int numThreshold = 3;
	private FilterKey fk = new FilterKey();
	private long beginTime = -1;
	private int rept = 0;
	private List<Text> valueList = new ArrayList<Text>();
	
	@Override
	protected void reduce(FilterKey filterKey, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		
		for(Text value : values){
			if (fk != null)
				System.out.println(filterKey + "\t" + fk + "\t" + filterKey.clkEquals(fk));
			System.out.println(filterKey);
			
			long clkTime = filterKey.getTime().get();
			
			if(beginTime == -1){
				beginTime = clkTime;
				rept ++;
				fk.set(filterKey);
				context.write(NullWritable.get(), value);
			}else if(filterKey.clkEquals(fk) && clkTime - beginTime < timeThreshold){
				rept ++;
				valueList.add(value);
			}else {
				if(rept < numThreshold){
					for(Text val: valueList){
						context.write(NullWritable.get(), val);
					}
				}
				System.out.println("in else:" + filterKey.toString());
				beginTime = filterKey.getTime().get();
				fk.set(filterKey);
				rept = 1;
				valueList.clear();
				context.write(NullWritable.get(),value);
			}
		}
		
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		if(rept < numThreshold){
			for(Text value: valueList){
				context.write(NullWritable.get(), value);
			}
		}
	}
}
