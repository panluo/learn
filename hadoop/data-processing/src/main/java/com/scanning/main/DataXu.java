package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Config;
import utils.DateTransformer;
import utils.Spliter;


public class DataXu extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		if(args.length<3){
			System.out.println("main.DataXu <input> <output> <confPath>");
			System.exit(0);
		}
		ToolRunner.run(new DataXu(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		conf.set("ConfPath", args[2]);
		
		Job job = Job.getInstance(conf,"DataXu scan");
		job.setJarByClass(DataXu.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(DataXuMap.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return 0;
	}

}

class DataXuMap extends Mapper<LongWritable, Text,Text,NullWritable>{

	Map<String, Integer> proper = null;
	ArrayList<String> line_item = null;
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		ArrayList<String> str = Spliter.splits(value.toString(), "\001");
		if(str.size() < proper.size()){
			System.out.println("line length: "+str.size() + "  proper length: " + proper.size());
			System.out.println(value.toString());
			context.getCounter("Error_log", "shorter_then_require").increment(1);
		}else{
			ArrayList<String> items = new ArrayList<String>();
			for(int i=0; i < line_item.size(); i++){
				items.add(str.get(proper.get(line_item.get(i))));
			}
			String time = DateTransformer.TimeFormatter(items.get(proper.get("time")));
			items.set(line_item.indexOf("time"), time);
			items.set(line_item.indexOf("campaign_id"),"0Duv9P9Muq");
			items.set(line_item.indexOf("line_item_id"), "0FSYFjIBIR");
			items.set(line_item.indexOf("time_zone"), "UTC+8");
			items.add("CNY");
			
			StringBuilder sb = new StringBuilder();
			for(String tmp : items){
				sb.append(tmp);
				sb.append("\t");
			}
			
			Text keyy = new Text(sb.toString().substring(0, sb.toString().length()-1));
			context.write(keyy, NullWritable.get());
		}
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Config.getInstance().LoadConf(conf.get("ConfPath"),"dataXu");
		proper = Config.getProper();
		line_item = Config.getItems();
		String keys = Config.getLineName();
		
		context.write(new Text(keys.substring(0,keys.length()-1)), NullWritable.get());
	}
	
}
