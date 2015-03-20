package com.bilin.clkfilter;

import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FilterProcessor extends Configured implements Tool {

	public static final String LOGTYPE = "logType";
    public static final String PROPERTY_FILE_PATH = "property_file_path";
    
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		FileSystem fs = FileSystem.get(conf);
	    fs.delete(new Path(args[1]), true);          //
	    
		conf.set(LOGTYPE, args[2]);                  
        conf.set(PROPERTY_FILE_PATH,args[3]);
        
        Calendar c = Calendar.getInstance();
        
		Job job = Job.getInstance(conf, args[2] + "_" + c.get(Calendar.YEAR) 
				+ (c.get(Calendar.MONTH) + 1) + c.get(Calendar.DAY_OF_MONTH) 
				+ c.get(Calendar.HOUR_OF_DAY));
		
		job.setJarByClass(FilterProcessor.class);
		job.setMapperClass(FilterMapper.class);
		
		job.setMapOutputKeyClass(FilterKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(FilterReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);						
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	 public static void main(String[] args) throws Exception {
	        if (4 > args.length) {
	            System.err.println("Missing required parameter!");
	            System.err.println("parameters: input_path output_path logType properties_file_path");
	            System.exit(2);
	        }
	        runJob(args);
	 }
	 
	 public static void runJob(String[] args) throws Exception {
	        ToolRunner.run(new Configuration(), new FilterProcessor(), args);
	 }

}
