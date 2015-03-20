package com.bilin.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.bilin.main.FreqConfig;
import com.bilin.utils.ReduceTools;

public class FreqReducer extends Reducer<Text, IntWritable, Text, Text>{
	private static final String PATH = "confPath";
	private static final String FRQ = ".frq";
	private static final String CNT = ".cnt";
	static Map<String,Map<Integer,String>> tmp = new HashMap<String,Map<Integer,String>>();
	private MultipleOutputs<Text, Text> multipleOutputs;
	private ReduceTools reduceTool = new ReduceTools();
	private String date = null;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		date = "."+context.getConfiguration().get("time", reduceTool.getYest());
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
		try{
			Configuration conf = context.getConfiguration();
			FreqConfig.getInstance().loadConfig(conf.get(PATH));
			tmp = FreqConfig.getInstance().getFreqMap();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		String filename = "";
		String keyInFile = "";
		
		if(key.toString().contains("-&-")){
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
		}
		int sum = 0;
		for(IntWritable val : value){
			sum += val.get();
		}
		
		if(!filename.isEmpty())
			multipleOutputs.write(new Text(keyInFile), new Text(sum+""), filename);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		Set<String> excs = FreqConfig.getInstance().getExchanges();
		Counter counter;
		for(String ex : excs){
			Map<Integer,String> freq = tmp.get(ex);
			for(Iterator<Integer> iter = freq.keySet().iterator();iter.hasNext();){	
				String filename = ex+"."+freq.get(iter.next())+date;
				counter = context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", filename+FRQ);
				multipleOutputs.write(new Text("KINDS"), new Text(counter.getValue()+""), filename+CNT);
			}
			
		}
//		for(String ex : excs){
//			String filename_geo = ex+".GEO"+date;
//			counter = context.getCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", filename_geo+FRQ);
//			multipleOutputs.write(new Text("KINDS"), new Text(counter.getValue()+""), filename_geo+CNT);
//		}
		multipleOutputs.close();
	}
}
