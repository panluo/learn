package com.bilin.main;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import com.bilin.job.FreqCombine;
import com.bilin.job.FreqMapper;
import com.bilin.job.FreqReducer;


enum Process_time {
	time00_30,time30_60,time60_90,time90_00;
}

public class FreqConf extends Configured implements Tool {

	private static final String PATH = "confPath";
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String confPath;
		String datetime;
		if(args.length >= 4)
			confPath = args[3];
		else
			confPath = "conf/FRE.properties";
//			confPath = "hdfs://namenode.bilintechnology.net:8020/conf/bilin.properties";
//			confPath = "hdfs://namenode.bilintechnology.net:8020/conf/FRE_domain.properties";
		if(args.length==6){
			datetime=args[5];
			conf.set("time", datetime);
		}
		conf.set(PATH, confPath);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		
		Job job = Job.getInstance(conf, "Frequency");
		job.setJarByClass(Processor.class);
		job.setMapperClass(FreqMapper.class);
		job.setCombinerClass(FreqCombine.class);
		job.setReducerClass(FreqReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
//		job.setNumReduceTasks(1);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.addCacheFile(URI.create(args[4]));
		//Important
		MultipleOutputs.setCountersEnabled(job, true);
		job.waitForCompletion(true);
		
		//单独输出processTime 计数
		String filename = "ProcessTime";
		Path filePath = new Path(args[1],filename);
		FSDataOutputStream out = fs.create(filePath);
		String bt = "";
		Counter counter;
		for(Process_time val : Process_time.values()){
			counter = job.getCounters().findCounter("process_time", val.name());
			bt = bt + val.name() + "\t" + counter.getValue() + "\n";
		}
		counter = job.getCounters().findCounter("LINE_NUM", "Total");
		bt = bt + "LINES\t" + counter.getValue() + "\n";
		out.write(bt.getBytes());
		IOUtils.closeStream(out);
		return 0;
	}
}
