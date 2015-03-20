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

public class ScanLog extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf =  getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]),true);
		
		conf.set("ConfPath", args[2]);
		
		Job job = Job.getInstance(conf, "Scanning log");
		job.setJarByClass(ScanLog.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(ScanMap.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 3){
			System.out.println("ScanLog <input> <output> <confPath>");
			System.exit(0);
		}
		ToolRunner.run(new ScanLog(), args);
	}

}

class ScanMap extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	Map<String, Integer> proper = null;
	ArrayList<String> line_item = null;
	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Config.getInstance().LoadConf(conf.get("ConfPath"),"prelytix");
		proper = Config.getProper();
		line_item = Config.getItems();
		String keys = Config.getLineName();
		
		context.write(new Text(keys.substring(0,keys.length()-1)), NullWritable.get());
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		ArrayList<String> str = Spliter.splits(value.toString(), "\t");
		if(str.size() < proper.size()){
			System.out.println("line length: "+str.size() + "  proper length: " + proper.size());
			System.out.println(value.toString());
			context.getCounter("Error_log", "shorter_then_require").increment(1);
		}else{
			ArrayList<String> items = new ArrayList<String>();
			for(int i=0; i < line_item.size(); i++){
				items.add(str.get(proper.get(line_item.get(i))));
			}
			//time
			String ESTTime = DateTransformer.UTCToEST(str.get(proper.get("time")), str.get(proper.get("time_zone")));
			items.set(line_item.indexOf("time"), ESTTime);
			
			//geo_info
			String[] geo_info = items.get(line_item.indexOf("geo")).split("\\|");
			String geo = "";
			if(geo_info.length != 0){
				for(int i=0; i < geo_info.length; i++){
					geo = geo.concat(geo_info[i]+"\t");
				}
				for (int j=0; j < 4-geo_info.length; j++)
					geo = geo.concat("\t");
					
				geo = geo.substring(0,geo.length()-1);
			}else{
				geo = "\t\t\t";
			}
			items.add(line_item.indexOf("user_ip")+1, geo);
			items.remove(line_item.indexOf("geo")+1);
			
			//domain
//			String domain = Spliter.getDomain(items.get(line_item.indexOf("url")));
//			items.add(line_item.indexOf("url"), domain);
			
			//no_flash
			String bool = items.get(items.size()-1);
			if(bool.equalsIgnoreCase("true"))
				items.set(items.size()-1, "no");
			else if(bool.equalsIgnoreCase("false"))
				items.set(items.size()-1, "yes");
			else 
				items.set(items.size()-1,bool);
			
			//no_sync
			String with_sync = "";
			if(items.get(line_item.indexOf("bilin_user_id")).isEmpty())
				with_sync = "1";
			else
				with_sync = "0";
			items.add(items.size(), with_sync);
			
			
			//currency
			items.add(items.size(),"USD");
			
			StringBuffer sb = new StringBuffer();
			for(String tmp : items){
				sb.append(tmp);
				sb.append("\t");
			}
			Text keyy = new Text(sb.toString().substring(0, sb.toString().length()-1));
			context.write(keyy, NullWritable.get());
		}	
	}	
}
