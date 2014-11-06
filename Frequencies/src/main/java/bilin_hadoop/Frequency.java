package bilin_hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
//import java.util.StringTokenizer;


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
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Frequency extends Configured implements Tool{
	public static final String TOTAL = "total";
	public static final String FRQ = ".frq";
	public static final String CNT = ".cnt";
	public static final String LOGTYPE = "logType";
	public static final String PATH = "property_file_path";
	public static final String EMPTY = "empty";
	public static final String LIST = "list";
	public static final String PART = "-r-00000";
	private static Map<Integer, String> logformat = null;
	static ArrayList<String> splits = new ArrayList<String>();

	public static void main(String[] args) throws Exception{
		if(args.length < 2){
			System.out.println("Usage Frequency <in> <out>");
			System.exit(2);
		}
		ToolRunner.run(new Frequency(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]),true);
		
		//指定配置文件位置
		String confPath;
		if(args.length==3)
			confPath = args[2];
		else
			confPath = "hdfs://namenode.bilintechnology.net:8020/conf/FRE.properties";
//			confPath = "conf/FRE.properties";
//			confPath = "hdfs://namenode.bilintechnology.net:8020/user/luo/domain.properties";

		conf.set(LOGTYPE,"freq");
		conf.set(PATH, confPath);
		
		//设置任务
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Frequency");
		job.setJarByClass(Frequency.class);
		job.setMapperClass(FqMap.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(FqReduce.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//输出方式
		MultipleOutputs.setCountersEnabled(job, true);
		job.waitForCompletion(true);
		
		conf.setBoolean("dfs.support.append", true);
		Counters counter = job.getCounters();
		//加载配置文件用以获得计数器名
		Config.getInstance().loadConfig(conf.get(PATH));
		logformat = Config.getInstance().getFreqFormat("baidu");
		int i=0;
		String date = getDate();
		Iterator<Integer> it = logformat.keySet().iterator();

		while(it.hasNext()){
			i = it.next();
			String filename = logformat.get(i)+date;
			//获取计数器
			Counter attrCounter = counter.findCounter(filename+FRQ, logformat.get(i));
			byte[] bt = ("KINDS"+"\t"+attrCounter.getValue()).getBytes();
			
			//写文件。两种方式
			//BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.append(new Path(args[1], filename+CNT+PART))));
			Path file = new Path(args[1], filename+CNT+PART);
			FSDataOutputStream out;
			if(!fs.exists(file)){
				out = fs.create(file);
				bt = ("TATOLS"+"\t"+"0"+"\n"+"KINDS"+"\t"+attrCounter.getValue()).getBytes();
			}else{
				out = fs.append(file);
			}
			out.write(bt);
			IOUtils.closeStream(out);
		}
		return 0;
	}
	/*
	 * 获取昨天的日期
	 */
	public static String getDate(){
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DATE, -1);
		Date d = c.getTime();
		SimpleDateFormat sp = new SimpleDateFormat("yyyyMMdd");
		String yesterday="."+sp.format(d);
		return yesterday;
	}

	static class FqMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				//加载配置文件
				Config.getInstance().loadConfig(context.getConfiguration().get(PATH));
			} catch (Exception e) {
				e.printStackTrace();
			}
			logformat = Config.getInstance().getFreqFormat("baidu");
		}

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
//			String values = value.toString().replaceAll("\t\t", "\t \t");
			StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			
			int i = 0;
			if(itr.hasMoreTokens()){
				itr.nextToken();
			}
			while(itr.hasMoreTokens()){
				
				String tmp = itr.nextToken(); 
				if(logformat.get(i)!=null){
					String attribute = logformat.get(i);
				
					if(tmp.equals(" ") || tmp.equals("")){
						context.write(new Text(attribute+"."+EMPTY), new IntWritable(1));
					}else{
						if(attribute.equals("referer") || attribute.equals("domain")){
							String[] domain = tmp.split("/");
							if(domain.length>=3){
								String[] simply_domain=domain[2].split("www.");
								if(simply_domain.length>= 2)
									tmp = simply_domain[1];
								else
									tmp = simply_domain[0];
							}else if(tmp.equals("")){
								System.out.println(tmp);
								tmp="unknown";
							}
//							System.out.println(attribute+tmp);
						}else if(attribute.equals("creative_type")){
							if(tmp.equals("1|2")){
								attribute = "1";
								context.write(new Text(attribute+"-&-2"), new IntWritable(1));
							}
						}
						context.write(new Text(attribute + "-&-" + tmp), new IntWritable(1));
						context.write(new Text(attribute), new IntWritable(1));
					}
				}
				i++;
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
		
	static class FqReduce extends Reducer<Text, IntWritable, Text, Text>{
		
		private MultipleOutputs<Text, Text> multipleOutputs;
		
		@Override
		protected void setup(
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			
			
			//OutputCollector collector = multipleOutputs.getCollector(namedOutput, reporter);
			String filename;
			String keyInFile;
			String date = getDate();
			
			if(key.toString().contains("-&-")){ //记录各属性单项值的个数 eg： domain--baidu.com
				String[] args = key.toString().split("-&-");
				filename = args[0] + date + FRQ;
				keyInFile = args[1];
				context.getCounter(filename, args[0]).increment(1);

			}else if(key.toString().contains(".empty")){ //记录各属性中空白值
				String[] args = key.toString().split(".empty");
				filename = args[0] + date + FRQ;
				keyInFile = key.toString();
			}else{   //记录各属性总数值
				filename = key.toString() + date + CNT;
				keyInFile = "TOTAL";
			}
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			// 以文件形式输出
			multipleOutputs.write(new Text(keyInFile), new Text(sum+""), filename);
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}
	public static ArrayList<String> splits(String str, String split){
		int begin = 0;
		int len = str.length();
		int end = str.indexOf(split, begin);
		splits.clear();
		while(begin != len && -1 != end){
			splits.add(str.substring(begin, end));
			begin = end+split.length();
			end = str.indexOf(split, begin);
		}
		splits.add(str.substring(begin, len));
		return splits;
	}
}