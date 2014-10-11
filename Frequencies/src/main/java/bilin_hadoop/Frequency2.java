package bilin_hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class Frequency2 extends Configured implements Tool {

	private static final String LOGTYPE = "logtype";
	private static final String PATH = "confPath";
	private static final String SPLIT = "\t";
	private static final String FRQ = ".frq";
	private static final String CNT = ".cnt";
	private static Map<Integer, String> freqFormat = null;
	static ArrayList<String> spl = new ArrayList<String>();
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		String confPath;
		String datetime;
		if(args.length >= 3)
			confPath = args[2];
		else
//			confPath = "conf/FRE.properties";
			confPath = "hdfs://namenode.bilintechnology.net:8020/conf/FRE.properties";
//			confPath = "hdfs://namenode.bilintechnology.net:8020/user/luo/domain.properties";
		if(args.length==4){
			datetime=args[3];
			conf.set("time", datetime);
		}
		conf.set(LOGTYPE, "freq");
		conf.set(PATH, confPath);
		Config.getInstance().loadConfig(conf.get(LOGTYPE),conf.get(PATH));
		freqFormat = Config.getInstance().getFreqFormat();
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Frequency2");
		job.setJarByClass(Frequency2.class);
		job.setMapperClass(FrqMap.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(FrqReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Important
		MultipleOutputs.setCountersEnabled(job, true);
		return job.waitForCompletion(true)? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("Usage Frequency <in> <out>");
			System.exit(2);
		}
		ToolRunner.run(new Frequency2(), args);
	}
	
	public static class FrqMap extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			try{
				Configuration conf = context.getConfiguration();
				Config.getInstance().loadConfig(conf.get(LOGTYPE),conf.get(PATH));
				freqFormat = Config.getInstance().getFreqFormat();
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			ArrayList<String> str = splits(value.toString(),SPLIT);
			if(str.size() < Config.getInstance().getLogFormatMap().size()-1){
				System.out.println("number of split : "+value.toString());
				context.getCounter("Error_log", "count").increment(1);
//			StringTokenizer itrs = new StringTokenizer(value.toString(),SPLIT);
//			if(Config.getInstance().getLogFormatMap().size() > itrs.countTokens() 
//					&& Config.getInstance().getLogFormatMap().size()!=0){
//				System.out.println(value.toString());
			}
			else{
			
				int i=0;
				for(String attribute : str){
					if(freqFormat.get(i) != null){
						String name = freqFormat.get(i);
						context.getCounter("LINE_NUM", name).increment(1);
						if(attribute.equals(" ") || attribute.equals("")){
							context.write(new Text(name+"."+"empty"), new IntWritable(1));
						}else{ 
							if(name.equals("domain") || name.equals("referer")){
								String[] domain = attribute.split("/");
								if(domain.length>=3){
									String[] simply_domain=domain[2].split("www.");
									if(domain[2].split("www.").length >= 2)
										attribute = simply_domain[1];
									else
										attribute = simply_domain[0];	
								}else if(attribute.equals("")){
									System.out.println(attribute);
									attribute = "unknown";
								}
							}else if(name.equals("creative_type")){
								if(attribute.equals("1|2")){
									attribute = "1";
									context.write(new Text(name+"-&-2"),new IntWritable(1));
								}
							}
							context.write(new Text(name+"-&-"+attribute), new IntWritable(1));
							context.getCounter("TOTALS",name).increment(1);
						}
					}
					i++;
				}
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int i = 0;
			Iterator<Integer> it = freqFormat.keySet().iterator();
		
			while(it.hasNext()){
				i = it.next();
				String name = freqFormat.get(i);
				//获取计数器
				Counter counter = context.getCounter("TOTALS",name);
				Counter lines = context.getCounter("LINE_NUM",name);
				context.write(new Text(name), new IntWritable((int)counter.getValue()));
				context.write(new Text("LINES"+"-"+name), new IntWritable((int)lines.getValue()));
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
		
		private MultipleOutputs<Text, Text> multipleOutputs;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
			try{
				Configuration conf = context.getConfiguration();
				Config.getInstance().loadConfig(conf.get(LOGTYPE),conf.get(PATH));
				freqFormat = Config.getInstance().getFreqFormat();
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
			
			if(key.toString().contains("-&-")){ //记录各属性单项值的个数 eg： domain--baidu.com
				String[] args = key.toString().split("-&-");
				filename = args[0] + date + FRQ;
				if(args.length != 2){
					System.out.println(key.toString());
					keyInFile = "empty";
				}
				else
					keyInFile = args[1];
				context.getCounter(filename, args[0]).increment(1);

			}else if(key.toString().contains(".empty")){ //记录各属性中空白值
				String[] args = key.toString().split(".empty");
				filename = args[0] + date + FRQ;
				keyInFile = key.toString();
			}else if(key.toString().contains("LINES-")){
				String[] args = key.toString().split("LINES-");
				filename = args[1] + date + CNT;
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
			int i = 0;
			String date = "." + context.getConfiguration().get("time", getDate());
	
			Iterator<Integer> it = freqFormat.keySet().iterator();
			while(it.hasNext()){
				i = it.next();
				String filename = freqFormat.get(i)+date;
				//获取计数器
//				if(job == null)
//					System.out.println("job null");
//				else{
//				Counter totalcounter = job.getCounters().findCounter("TOTALS", "domain");//freqFormat.get(i));
//				multipleOutputs.write(new Text("TOTALS"), new Text(totalcounter.getValue()+""), filename+CNT);
				Counter counter = context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", filename+FRQ);
				multipleOutputs.write(new Text("KINDS"), new Text(counter.getValue()+""), filename+CNT);
//				}
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
