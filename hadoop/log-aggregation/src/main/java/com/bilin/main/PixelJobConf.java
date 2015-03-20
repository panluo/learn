package com.bilin.main;

import com.bilin.job.PixelLogProcessMapper;
import com.bilin.job.PixelLogProcessReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import java.util.Calendar;

public class PixelJobConf extends Configured implements Tool{

	public static final String LOGTYPE = "logType";
    public static final String PROPERTY_FILE_PATH = "property_file_path";
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//delete output path if exist
//		FileSystem fs = FileSystem.get(conf);
//	    fs.delete(new Path(args[1]), true);          //
	    
		conf.set(LOGTYPE, args[2]);                  
        conf.set(PROPERTY_FILE_PATH,args[3]);
        
        Calendar c = Calendar.getInstance();
        
		Job job = Job.getInstance(conf, args[2] + "_" + c.get(Calendar.YEAR) 
				+ (c.get(Calendar.MONTH) + 1) + c.get(Calendar.DAY_OF_MONTH) 
				+ c.get(Calendar.HOUR_OF_DAY));
		job.setJarByClass(PixelProcessor.class);
		job.setMapperClass(PixelLogProcessMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PixelLogProcessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(7);						//
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		MultipleOutputs.setCountersEnabled(job, true);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
