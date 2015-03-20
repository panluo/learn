package main;

import java.net.URI;

import mapred.UrlMapper;
import mapred.UrlReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Processer_url extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(new URI(args[1]),conf);
		fs.delete(new Path(args[1]), true);
		conf.set("CONFPATH", args[2]);
		
		Job job = Job.getInstance(conf,"url scan");
		job.setJarByClass(Processer_url.class);
		job.setMapperClass(UrlMapper.class);
		job.setReducerClass(UrlReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		if(args.length < 3){
			System.err.println("Stage <input> <output> <confpath>");
			System.exit(0);
		}
		ToolRunner.run(new Processer_url(), args);
	}
}
