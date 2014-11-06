package bilin_hadoop;

import ip.IpToGeo;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

enum Process_time {
	time00_30,time30_60,time60_90,time90_00;
}

public class Frequency2 extends Configured implements Tool {

	private static final String PATH = "confPath";
	private static final String SPLIT = "\t";
	private static final String FRQ = ".frq";
	private static final String CNT = ".cnt";
	private static Map<String, Integer> reqFormat = null;
	static ArrayList<String> spl = new ArrayList<String>();
	private static IntWritable ONE = new IntWritable(1);
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		String confPath;
		String geoPath;
		String datetime;
		if(args.length >= 3 && args[2]!=" ")
			confPath = args[2];
		else
			confPath = "conf/FRE.properties";
//			confPath = "hdfs://namenode.bilintechnology.net:8020/conf/FRE.properties";
//			confPath = "hdfs://namenode.bilintechnology.net:8020/user/luo/domain.properties";
		if(args.length >=4 && args[3]!=" ")
			geoPath=args[3];
		else
			geoPath="hdfs://namenode.bilintechnology.net:8020/user/bilinhadoop/ip_geo/china.csv";
		
		if(args.length==5){
			datetime=args[4];
			conf.set("time", datetime);
		}
		conf.set(PATH, confPath);
	
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		
		Job job = Job.getInstance(conf, "Frequency2");
		job.setJarByClass(Frequency2.class);
		job.setMapperClass(FrqMap.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(FrqReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
//		job.setNumReduceTasks(1);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.addCacheFile(URI.create(geoPath));
		//Important
		MultipleOutputs.setCountersEnabled(job, true);
		job.waitForCompletion(true);
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

	public static void main(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("Usage Frequency <in> <out>");
			System.exit(2);
		}
		ToolRunner.run(new Frequency2(), args);
	}
	
	public static class FrqMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		static Map<String,Map<Integer,String>> tmp = new HashMap<String,Map<Integer,String>>();
		static Map<Integer, String> freq = null;
		static Set<String> excs = null;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			try{
				Configuration conf = context.getConfiguration();
				Config.getInstance().loadConfig(conf.get(PATH));
				reqFormat = Config.getInstance().getLogFormatMap();
				tmp = Config.getInstance().getFreqMap();
				excs =  Config.getInstance().getExchanges();
				URI[] localCacheFile = context.getCacheFiles();                //get ip to geo code file china.csv and loading
		        FileSystem fs = FileSystem.get(localCacheFile[0], new Configuration());
		        IpToGeo.loadGeoFile(localCacheFile[0].toString(),fs);
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			ArrayList<String> str = splits(value.toString(),SPLIT);
			int len = Config.getInstance().getLogFormatMap().size();
			if(str.size()!= len && str.size() != len+3 && str.size()+2 != len){
				System.out.println("number of split : "+ str.size() + 
						" number of format : " + len);
				System.out.println("the line :" +value.toString());
				context.getCounter("Error_log", "count").increment(1);
			}
			else{
				String exchange = str.get(reqFormat.get("ad_exchange"));
				
				if(!excs.contains(exchange)){
					System.out.println(str.toString());
					context.getCounter("Error_log", "WithOutExchange").increment(1);
				}
				else{
					freq = tmp.get(exchange);

//				double process_time = 0.0;
//				process_time = Double.valueOf(str.get(reqFormat.get("process_time"))).doubleValue();
//				if(process_time < 30)
//					context.getCounter("process_time",Process_time.time00_30.toString()).increment(1);
//				else if(process_time >=30 && process_time < 60)
//					context.getCounter("process_time",Process_time.time30_60.toString()).increment(1);
//				else if(process_time >=60 && process_time < 90)
//					context.getCounter("process_time",Process_time.time60_90.toString()).increment(1);
//				else if(process_time >= 90)
//					context.getCounter("process_time",Process_time.time90_00.toString()).increment(1);
//				
					int i=0;
					context.getCounter("LINE_NUM", "Total").increment(1);
					for(Iterator<Integer> at = freq.keySet().iterator();at.hasNext();){
						i = at.next();
						String name = freq.get(i);
						String fulname = exchange + "_" + name;
						String attribute = str.get(i);
						if(attribute.equals(" ") || attribute.equals("")){
							context.write(new Text(fulname+"."+"empty"), ONE);
						}else{
							if(name.equals("domain") || name.equals("referer")){
								String result = null;
								StringTokenizer domain = new StringTokenizer(attribute,"/");
								if(domain.countTokens()>=2){
									domain.nextToken();
									result = domain.nextToken();
									if(result.startsWith("www."))
										attribute = result.substring(4);
									else
										attribute = result;
								}
								context.write(new Text(fulname+"-&-"+attribute), ONE);
							}else if(name.equals("creative_type")){
								if(attribute.contains("|")){
									StringTokenizer type = new StringTokenizer(attribute,"\\|");									while(type.hasMoreTokens())
										context.write(new Text(fulname+"-&-"+type.nextToken()), ONE);
								}else
									context.write(new Text(fulname+"-&-"+attribute), ONE);
							}else if(name.equals("user_ip")){
								context.write(new Text(fulname+"-&-"+attribute), ONE);
								String geoinfo = IpToGeo.getGeo(attribute);
								context.write(new Text(exchange+"_GEO-&-"+geoinfo),ONE);
							}else{
								context.write(new Text(fulname+"-&-"+attribute), ONE);
							}
							context.getCounter("TOTALS",fulname).increment(1);
						}
					}
				}
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for(String exc : excs){
				Map<Integer,String> freq_exc = tmp.get(exc); 
				int i = 0;
				Iterator<Integer> it = freq_exc.keySet().iterator();
				Counter counter;
				while(it.hasNext()){
					i = it.next();
					String name = freq_exc.get(i);
					String fulname = exc+"_"+name;
					counter = context.getCounter("TOTALS",fulname);
					
					context.write(new Text(fulname), new IntWritable((int)counter.getValue()));
					Counter lines = context.getCounter("LINE_NUM","Total");
					context.write(new Text("LINES-"+fulname), new IntWritable((int)lines.getValue()));
				}
			}
		}	
	}
	static class Combine extends Reducer<Text,IntWritable, Text, IntWritable>{

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
	
	static class FrqReduce extends Reducer<Text, IntWritable, Text, Text>{
		
		static Map<String,Map<Integer,String>> tmp = new HashMap<String,Map<Integer,String>>();
		private MultipleOutputs<Text, Text> multipleOutputs;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
			try{
				Configuration conf = context.getConfiguration();
				Config.getInstance().loadConfig(conf.get(PATH));
				tmp = Config.getInstance().getFreqMap();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			String filename;
			String keyInFile;
			String date = "."+context.getConfiguration().get("time", getDate());
			
			if(key.toString().contains("-&-")){ //记录各属性单项值的个数 eg： domain-&-baidu.com
				String[] args = key.toString().split("-&-");
				filename = args[0] + date + FRQ;
				if(args.length != 2){
					System.out.println(key.toString());
					keyInFile = "empty";
				}
				else
					keyInFile = args[1];

			}else if(key.toString().contains(".empty")){ //记录各属性中空白值
				int len = key.toString().length();
				filename = key.toString().substring(0,len-6) + date + FRQ;
				keyInFile = key.toString();
			}else if(key.toString().contains("LINES-")){
				filename = key.toString().substring(6) + date + CNT;
				keyInFile = "LINES";
			}else{
				filename = key.toString() + date + CNT;
				keyInFile = "TOTALS";
			}
			int sum = 0;
			for(IntWritable val : value){
				sum += val.get();
			}
			
			// 以文件形式输出
			multipleOutputs.write(new Text(keyInFile), new Text(sum+""), filename);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {

			Set<String> excs = Config.getInstance().getExchanges();
			String date = "." + context.getConfiguration().get("time", getDate());
			Counter counter;

			for(String ex : excs){
				Map<Integer,String> freq = tmp.get(ex);
				for(Iterator<Integer> iter = freq.keySet().iterator();iter.hasNext();){	
					String filename = ex+"_"+freq.get(iter.next())+date;
					counter = context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", filename+FRQ);
					multipleOutputs.write(new Text("KINDS"), new Text(counter.getValue()+""), filename+CNT);
				}
				
			}
			for(String ex : excs){
				String filename_geo = ex+"_GEO"+date;
				counter = context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", filename_geo+FRQ);
				multipleOutputs.write(new Text("KINDS"), new Text(counter.getValue()+""), filename_geo+CNT);
			}
			multipleOutputs.close();
		}
		
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
	
	public static ArrayList<String> splits(String str, String split){
		int begin = 0;
		int len = str.length();
		int end = str.indexOf(split, begin);
		spl.clear();
		while(begin != len && -1 != end){
			spl.add(str.substring(begin, end));
			begin = end+split.length();
			end = str.indexOf(split, begin);
		}
		spl.add(str.substring(begin, len));
		return spl;
	}
}
