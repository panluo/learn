package bilin_hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

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
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		String confPath = "conf/FRE.properties";
		String confPath = "hdfs://master.bilintechnology.net:8020/conf/FRE.properties";
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
			String values = value.toString().replaceAll("\t\t", "\t \t");
			StringTokenizer itrs = new StringTokenizer(values,SPLIT);
			if(itrs.hasMoreTokens())
				itrs.nextToken();
			
			int i=0;
			while(itrs.hasMoreTokens()){
				String attribute = itrs.nextToken();
				if(freqFormat.get(i) != null){
					String name = freqFormat.get(i);
					if(!attribute.equals(" ")){
						if(name.equals("referer")){
							String[] domain = attribute.split("/");
							if(domain.length>=3){
								attribute = domain[2];
								System.out.println(attribute);
							}
						}
						context.write(new Text(name+"--"+attribute), new IntWritable(1));
						context.getCounter("TOTALS",name).increment(1);
					}else{
						context.write(new Text(name+"."+"empty"), new IntWritable(1));
					}
				}
				i++;
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
				context.write(new Text(name), new IntWritable((int)counter.getValue()));
			}
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
			String date = getDate();
			
			if(key.toString().contains("--")){ //记录各属性单项值的个数 eg： domain--baidu.com
				String[] args = key.toString().split("--");
				filename = args[0] + date + FRQ;
				keyInFile = args[1];
				context.getCounter(filename, args[0]).increment(1);

			}else if(key.toString().contains(".empty")){ //记录各属性中空白值
				String[] args = key.toString().split(".empty");
				filename = args[0] + date + FRQ;
				keyInFile = key.toString();
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
//			Cluster cluster = new (context.getConfiguration());
//			Job job = cluster.getJob(context.getJobID());
//			Job job = (context.getJobName())
//			Cluster cluster = new Cluster(context.getConfiguration());
//			Job job = cluster.getJob(context.getJobID());
//			System.out.println(job.getJobName());
//			System.out.println(context.getJobID().toString()+"++++++++++++++++++++");
			
			Iterator<Integer> it = freqFormat.keySet().iterator();
			while(it.hasNext()){
				i = it.next();
				String filename = freqFormat.get(i)+getDate();
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
		String yesterday="."+sp.format(d);
		return yesterday;
	}
}
