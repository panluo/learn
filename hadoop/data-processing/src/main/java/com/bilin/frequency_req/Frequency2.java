package bilin_hadoop;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

	private static final String PATH = "confPath";
	private static final String SPLIT = "\t";
	private static final String FRQ = ".frq";
	private static final String CNT = ".cnt";
	private static Map<String, Integer> reqFormat = null;
	static ArrayList<String> spl = new ArrayList<String>();
	private static LongWritable ONE = new LongWritable(1);
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		String confPath;
		String datetime;
		if(args.length >= 3 && args[2]!=" ")
			confPath = args[2];
		else
			confPath = "conf/FRE.properties";
//			confPath = "hdfs://namenode.bilintechnology.net:8020/conf/FRE.properties";
//			confPath = "hdfs://namenode.bilintechnology.net:8020/user/luo/domain.properties";
		
		if(args.length==4){
			datetime=args[3];
			conf.set("time", datetime);
		}
		conf.set(PATH, confPath);
		
//		FileSystem fs = FileSystem.get(new URI(args[1]), conf);
//		fs.delete(new Path(args[1]), true);
		
		Job job = Job.getInstance(conf, "Frequency");
		
		job.setJarByClass(Frequency2.class);
		job.setMapperClass(FrqMap.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(FrqReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
//		job.setNumReduceTasks(1);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
//		job.addCacheFile(URI.create(geoPath));
		MultipleOutputs.setCountersEnabled(job, true);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("Usage Frequency <in> <out>");
			System.exit(2);
		}
		ToolRunner.run(new Frequency2(), args);
	}
	
	public static class FrqMap extends Mapper<LongWritable, Text, Text, LongWritable>{
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
//				URI[] localCacheFile = context.getCacheFiles();                //get ip to geo code file china.csv and loading
//		        FileSystem fs = FileSystem.get(localCacheFile[0], new Configuration());
//		        IpToGeo.loadGeoFile(localCacheFile[0].toString(),fs);
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			ArrayList<String> str = splits(value.toString(),SPLIT);
			int len = Config.getInstance().getLogFormatMap().size();
//			if(str.size() < len || str.size()+1 < len ){
			if(str.size() != 64){	
				System.out.println("number of split : "+ str.size() + 
						" number of format : " + len);
				System.out.println("the line :" +value.toString());
				context.getCounter("Error_log", "ShorterThenRequire").increment(1);
			}
			else{
				String exchange = str.get(reqFormat.get("ad_exchange"));

				if(!excs.contains(exchange)){
					System.out.println(value.toString());
					context.getCounter("Error_log", "WithOutExchange").increment(1);
				}else{
					
					freq = tmp.get(exchange);
					String first_name = exchange;
					String ssp = "";
					if(exchange.equals("bidswitch")){
						ssp = str.get(reqFormat.get("ssp"));
					}
					String ssp_name;
					if(ssp.isEmpty() || ssp.equals("+=+")){
						System.err.println(value.toString());
						context.getCounter("Error_log", "SSPError").increment(1);
						return;
					}else{
						ssp_name=ssp.concat("\t");
						context.write(new Text("bidswitch++"+ssp), ONE);
					}

					context.getCounter("LINE_NUM", first_name).increment(1);
					int i=0;
					for(Iterator<Integer> at = freq.keySet().iterator();at.hasNext();){
						i = at.next();
						String name = freq.get(i);
						String fulname = first_name + "." + name;
						String attribute = str.get(i).trim();
						
						if(attribute.equals(" ") || attribute.equals("")){
							context.write(new Text(fulname+"."+ssp_name+"empty"), ONE);
						}else{
//							if(name.equals("url") || name.equals("referer")){
//								String result = ssp_name+getDomain(attribute);
//								context.write(new Text(first_name+".domain"+"-&-"+result), ONE);
//							
							if(name.equals("referer")){
								String result = ssp_name+getDomain(attribute);
								context.write(new Text(fulname+"-&-"+ssp_name+result), ONE);
							}else if(name.equals("creative_type")){
								if(attribute.contains("|")){
									StringTokenizer type = new StringTokenizer(attribute,"\\|");
									while(type.hasMoreTokens())
										context.write(new Text(fulname+"-&-"+ssp_name+type.nextToken()), ONE);
								}else
									context.write(new Text(fulname+"-&-"+ssp_name+attribute), ONE);
							}else if(name.equals("geo") && exchange.equals("bidswitch")){
								String city;
								if(attribute.contains("|"))
									city = attribute.substring(0, attribute.lastIndexOf("|"));
								else
									city = attribute;
								
								context.write(new Text(fulname+"-&-"+ssp_name+city), ONE);
							}else if(name.equals("site_category")){
								if(attribute.contains("|")){
									StringTokenizer site = new StringTokenizer(attribute,"\\|");
									while(site.hasMoreTokens())
										context.write(new Text(fulname+"-&-"+ssp_name+site.nextToken()), ONE);
								}else
									context.write(new Text(fulname+"-&-"+ssp_name+attribute), ONE);
//							}else if(name.equals("")){
							}else{
								context.write(new Text(fulname+"-&-"+ssp_name+attribute), ONE);
							}
						}
					}
				}
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String name = "";
			for(String exc : excs){
				Map<Integer,String> freq_exc = tmp.get(exc); 
				int i = 0;
				Iterator<Integer> it = freq_exc.keySet().iterator();
				Counter lines = context.getCounter("LINE_NUM",exc);
				while(it.hasNext()){
					i = it.next();
					name = exc+"."+freq_exc.get(i);
//					if(freq_exc.get(i).equals("url"))
//						name = exc.concat(".").concat("domain");
					if(lines.getValue()!=0)
						context.write(new Text("LINES-"+name), new LongWritable(lines.getValue()));
				}
			}
		}	
	}
	static class Combine extends Reducer<Text,LongWritable, Text, LongWritable>{

		@Override
		protected void reduce(Text arg0, Iterable<LongWritable> arg1,Context arg2)
				throws IOException, InterruptedException {
			int sum = 0;
			
			for(LongWritable val : arg1){
				sum += val.get();
			}
			arg2.write(arg0, new LongWritable(sum));
		}		
	}
	
	static class FrqReduce extends Reducer<Text, LongWritable, Text, Text>{
		
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
		protected void reduce(Text key, Iterable<LongWritable> value, Context context)
				throws IOException, InterruptedException {
			String filename = "";
			String keyInFile = "";
			String date = "."+context.getConfiguration().get("time", getDate());
			
			int sum = 0;
			for(LongWritable val : value){
				sum += val.get();
			}
			
			if(key.toString().contains("-&-")){ //记录各属性单项值的个数 eg： domain-&-baidu.com
				String[] args = key.toString().split("-&-");
				filename = args[0] + date + FRQ;
				if(args.length < 2){
					System.out.println(key.toString());
					keyInFile = "empty";
				}
				else
					keyInFile = args[1];

			}else if(key.toString().contains("empty")){ //记录各属性中空白值
				String keyy = key.toString();
				filename = keyy.substring(0,keyy.lastIndexOf("."))+ date + FRQ;
				if(keyy.contains("\t"))
					keyInFile = keyy.substring(keyy.lastIndexOf(".")+1); // eg: bidswitch.domain.cox \t baidu.com
				else
					keyInFile = keyy.substring(keyy.indexOf(".")+1);	// eg: baidu.domain \t baidu.com
			}else if(key.toString().contains("LINES-")){
				filename = key.toString().substring(6) + date + CNT;
				keyInFile = key.toString().substring(6,key.toString().indexOf("."))+"\tLINES";
			}else if(key.toString().contains("bidswitch++")){
				Map<Integer,String> bidswitch = tmp.get("bidswitch");
				String item;
				for(Iterator<Integer> it = bidswitch.keySet().iterator();it.hasNext();){
					item = bidswitch.get(it.next());
//					if(item.equals("url"))
//						item = "domain";
					String name = "bidswitch".concat("."+item+date+CNT);
					multipleOutputs.write(new Text(key.toString().substring(11)+"\tLINES"), new Text(sum+""), name);
				}
				filename=keyInFile="";
			}	
			// 以文件形式输出
			if(!filename.isEmpty())
				multipleOutputs.write(new Text(keyInFile), new Text(sum+""), filename);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
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

	public static String getDomain(String url){
		if(url==null||url.trim().equals("")){
			return "unknown";
		}
		String domain = "";
		Pattern p = Pattern.compile("(?<=//)((\\w)+\\.)+\\w+");
		Matcher matcher = p.matcher(url);  
		if(matcher.find()){
			domain = matcher.group();
			if(domain.startsWith("www."))
				domain = domain.substring(4);
		}
		return domain;
	}
}
