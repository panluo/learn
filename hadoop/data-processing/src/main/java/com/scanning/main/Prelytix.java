package main;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.DateTransformer;
import utils.Prelytix_Config;
import utils.Spliter;

public class Prelytix extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
//		FileSystem fs = FileSystem.get(conf);
//		fs.delete(new Path(args[2]),true);
		
		conf.set("CONFPATH", args[3]);
		conf.set("RATE", args[4]);
		if(args.length >= 5)
			for(int cp = 0 ; cp < args.length-5 ; cp++){
				String name = "campaign_id"+cp;
				conf.set(name, args[5+cp]);
			}
		conf.set("num_campaign", (args.length-5)+"");
		Job job = Job.getInstance(conf,"Prelytix_req");
		job.setJarByClass(Prelytix.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);

		MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, ReqMap.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ImpMap.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		FileInputFormat.setInputDirRecursive(job, true);
		MultipleOutputs.setCountersEnabled(job, true);
		
		job.waitForCompletion(true);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 5){
			System.out.println("main.Prelytix <input> <output> <confPath> <rate> <campaign_id>");
			System.exit(0);
		}
		ToolRunner.run(new Prelytix(), args);
	}
	
	public static String getDate(){
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DATE, -1);
		Date d = c.getTime();
		//String yesterday = "."+c.get(Calendar.YEAR)+(c.get(Calendar.MONTH)+1)+c.get(Calendar.DAY_OF_MONTH);
		SimpleDateFormat sp = new SimpleDateFormat("yyyyMMdd");
		String yesterday=sp.format(d);
		return yesterday;
	}
}



class ReqMap extends Mapper<LongWritable, Text, Text, NullWritable>{

	Map<String,Integer> lineOfLog = null;
	ArrayList<String> format = null;
	MultipleOutputs<Text,NullWritable> multipleOutputs;
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		ArrayList<String> str = Spliter.splits(value.toString(), "\t");
		if(str.size() < lineOfLog.size()){
			System.out.println("line length: "+str.size() + "  proper length: " + lineOfLog.size());
			System.out.println(value.toString());
			context.getCounter("Error_log", "shorter_then_require").increment(1);
		}else{
			int rate = Integer.parseInt(context.getConfiguration().get("RATE"));
			int number = new Random().nextInt(100) + 1;
			
			int length = str.get(lineOfLog.get("url")).length();
			if(number <= rate && length <= 250){
				
				ArrayList<String> format_log = new ArrayList<String>();
				format_log.clear();
				for(int i=0; i < format.size(); i++){
					format_log.add("");
				}
				int i=0;
				for(String item : format){
					if(lineOfLog.containsKey(item)){
						format_log.set(i, str.get(lineOfLog.get(item)));
					}
					i++;
				}
				
				//time
				String ESTTime = DateTransformer.UTCToEST(str.get(lineOfLog.get("date_time")), str.get(lineOfLog.get("time_zone")));
				format_log.set(format.indexOf("date_time"), ESTTime);
				// geo
				ArrayList<String> geo = Spliter.splits(str.get(lineOfLog.get("geo")),"|");
				if(geo.size()==4){
					for(int j=0; j<4; j++){
						format_log.set(format.indexOf("user_country")+j, geo.get(j));
					}
				}
				format_log.set(format.indexOf("ip"), format_log.get(format.indexOf("user_ip")));
				format_log.set(format.indexOf("country"), format_log.get(format.indexOf("user_country")));
				format_log.set(format.indexOf("region"), format_log.get(format.indexOf("user_region")));
				
				// no flash
				String flash = str.get(lineOfLog.get("is_flash_allowed"));
				if(flash.equalsIgnoreCase("true"))
					format_log.set(format.indexOf("with_flash"), "yes");
				else if(flash.equalsIgnoreCase("false"))
					format_log.set(format.indexOf("with_flash"), "no");
				else
					format_log.set(format.indexOf("with_flash"),flash);
				
				String uuid = str.get(lineOfLog.get("uuid"));
				if(uuid.isEmpty())
					format_log.set(format.indexOf("with_sync"), "1");
				else
					format_log.set(format.indexOf("with_sync"), "0");
				
				format_log.set(format.indexOf("currency"),"USD");
				
				String line="";
				for(String val : format_log){
					line = line.concat(val.concat("\t"));
				}
				multipleOutputs.write(new Text(line.substring(0,line.length()-1)), NullWritable.get(),"outp/req_"+Prelytix.getDate());
//				context.write(new Text(line.substring(0,line.length()-1)), NullWritable.get());
			}else{
				context.getCounter("Error_log", "req_put_away").increment(1);
			}
		}
	}
	
	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
		try {
			Prelytix_Config.getInstance().loadConf(context.getConfiguration().get("CONFPATH"), "req");
			format = Prelytix_Config.getFormatLog();
			lineOfLog = Prelytix_Config.getLineOfLog();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		String line_file="";
		for(String item : format)
			line_file = line_file.concat(item.concat("\t"));
		multipleOutputs.write(new Text(line_file.substring(0, line_file.length()-1)), NullWritable.get(),"req_"+Prelytix.getDate());
//		context.write(new Text(line_file.substring(0, line_file.length()-1)), NullWritable.get());
	}
}


class ImpMap extends Mapper<LongWritable ,Text ,Text, NullWritable>{
	ArrayList<String> format_log = null;
	Map<String,Integer> lineOfLog = null;
	ArrayList<String> format = null;
	MultipleOutputs<Text,NullWritable> multipleOutputs;
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		ArrayList<String> str = Spliter.splits(value.toString(), "\t");
		if(str.size() < lineOfLog.size()){
			System.out.println("line length: "+str.size() + "  proper length: " + lineOfLog.size());
			System.out.println(value.toString());
			context.getCounter("Error_log", "shorter_then_require").increment(1);
		}else{
			String campaign_id_log=str.get(lineOfLog.get("campaign_id"));
			int nums = Integer.parseInt(context.getConfiguration().get("num_campaign"));
			int cp;
			for(cp=0;cp<nums;cp++){
				if(context.getConfiguration().get("campaign_id"+cp).equals(campaign_id_log))
					break;
			}
			if(cp != nums){
				format_log = new ArrayList<String>();
				format_log.clear();
				for(int i=0; i < format.size(); i++){
					format_log.add("");
				}
				int i=0;
				for(String item : format){
					if(lineOfLog.containsKey(item)){
						format_log.set(i, str.get(lineOfLog.get(item)));
					}
					i++;
				}
			
				String ESTTime = DateTransformer.UTCToEST(str.get(lineOfLog.get("date_time")), str.get(lineOfLog.get("time_zone")));
				format_log.set(format.indexOf("date_time"), ESTTime);
				// geo
				String geo_info = str.get(lineOfLog.get("geo"));
				ArrayList<String> geo = Spliter.splits(geo_info,"|");
				if(geo.size()==4){
					for(int j=0; j<4; j++){
						format_log.set(format.indexOf("user_country")+j, geo.get(j));
					}
				}
			
				format_log.set(format.indexOf("ip"), format_log.get(format.indexOf("user_ip")));
				format_log.set(format.indexOf("country"), format_log.get(format.indexOf("user_country")));
				format_log.set(format.indexOf("region"), format_log.get(format.indexOf("user_region")));
		
				String flash = str.get(lineOfLog.get("is_flash_allowed"));
				if(flash.equalsIgnoreCase("true"))
					format_log.set(format.indexOf("with_flash"), "yes");
				else if(flash.equalsIgnoreCase("false"))
					format_log.set(format.indexOf("with_flash"), "yes");
				else
					format_log.set(format.indexOf("with_flash"),flash);
				
				String uuid = str.get(lineOfLog.get("uuid"));
				if(uuid.isEmpty())
					format_log.set(format.indexOf("with_sync"), "0");
				else
					format_log.set(format.indexOf("with_sync"), "1");
				
				format_log.set(format.indexOf("currency"),"USD");
			
				String line="";
				for(String val : format_log){
					line = line.concat(val.concat("\t"));
				}
				multipleOutputs.write(new Text(line.substring(0,line.length()-1)), NullWritable.get(),"imp_"+Prelytix.getDate());
//				context.write(new Text(line.substring(0,line.length()-1)), NullWritable.get());
			}else
				context.getCounter("Error_log", "imp_put_away").increment(1);
		}
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
		try {
			Prelytix_Config.getInstance().loadConf(context.getConfiguration().get("CONFPATH"), "imp");
			format = Prelytix_Config.getFormatLog();
			lineOfLog = Prelytix_Config.getLineOfLog();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		String line_file="";
		for(String item : format)
			line_file = line_file.concat(item.concat("\t"));
		multipleOutputs.write(new Text(line_file.substring(0, line_file.length()-1)), NullWritable.get(),"imp_"+Prelytix.getDate());
//		context.write(new Text(line_file.substring(0, line_file.length()-1)), NullWritable.get());
	}
	
}

