package com.bilin.job;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FreqCombine extends Reducer<Text,IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		
		for(IntWritable val : arg1){
			sum += val.get();
		}
		arg2.write(arg0, new IntWritable(sum));
	}
}
