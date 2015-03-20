package com.bilin.geturl;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ScanUrl extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		PathGenerator gt = new PathGenerator();
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(new URI(args[1]), conf);
		fs.delete(new Path(args[1]),true);
		
		Job job = Job.getInstance();
		job.setJarByClass(ScanUrl.class);
		job.setMapperClass(UrlMapper.class);
		job.setReducerClass(UrlReducer.class);
		job.setCombinerClass(UrlReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
		String input = args[0];
		int days_num = Integer.parseInt(args[2]);
		for(int i=0; i < days_num; i++){
			
			MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class, UrlMapper.class);
		
			String newday = gt.newPath(input);
			System.out.println(input);
			if(newday!=null)
				input = newday;
			else
				break;
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 2)
			System.out.println("usage <input> <outpu>");
		ToolRunner.run(new ScanUrl(), args);
	}

}
class UrlMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer line = new StringTokenizer(value.toString(),"\t");
		if(line.countTokens() == 3){
			line.nextToken();
			context.write(new Text(line.nextToken()), new LongWritable(Long.parseLong(line.nextToken())));
		}else{
			context.getCounter("Error line", "Error length of line");
		}
	}
}

class UrlReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2)
			throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable tmp : arg1) {
			sum = sum + tmp.get();
		}
		if(sum > 10)
			arg2.write(arg0,new LongWritable(sum));
	}
}
