package com.bilin.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;


import com.bilin.main.FreqConfig;
import com.bilin.utils.MapTools;

enum Process_time {
	time00_30,time30_60,time60_90,time90_00;
}
public class FreqMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private static final String PATH = "confPath";
	private static Map<String, Integer> reqFormat = null;
	private static final String SPLIT = "\t"; 
	private static Set<String> Exchanges = new HashSet<String>();
	private static Map<String,Map<Integer,String>> tmp = new HashMap<String,Map<Integer,String>>();
	private static Map<Integer, String> freq = null;
	MapTools mapTools = new MapTools();
	private IntWritable ONE = new IntWritable(1); 
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		try{
			Configuration conf = context.getConfiguration();
			FreqConfig.getInstance().loadConfig(conf.get(PATH));
			tmp = FreqConfig.getInstance().getFreqMap();
			Exchanges =  FreqConfig.getInstance().getExchanges();
			reqFormat = FreqConfig.getInstance().getLogFormatMap();	
			
//			URI[] localCacheFile = context.getCacheFiles();                //get ip to geo code file china.csv and loading
//	        FileSystem fs = FileSystem.get(localCacheFile[0], new Configuration());
//	        IpToGeo.loadGeoFile(localCacheFile[0].toString(),fs);
		
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		ArrayList<String> str = mapTools.split(value.toString(),SPLIT);
		int len = FreqConfig.getInstance().getLogFormatMap().size();
		if(str.size() < len){
			System.out.println("number of split : "+str.size()+
					" while config length : "+FreqConfig.getInstance().getLogFormatMap().size());
			System.out.println(value.toString());
			context.getCounter("Error_log", "count").increment(1);
		}
		else{
			String exchange = str.get(reqFormat.get("ad_exchange"));
			
			if(!Exchanges.contains(exchange)){
				System.out.println(value.toString());
				context.getCounter("Error_log", "WithOutExchange").increment(1);
			}
			else{
				context.getCounter("LINE_NUM", exchange).increment(1);
				freq = tmp.get(exchange);
			
				int i=0;
				for(Iterator<Integer> at = freq.keySet().iterator();at.hasNext();){
					i = at.next();
					String name = freq.get(i);
					String fulname = exchange + "." + name;
					String attribute = str.get(i);
					if(attribute.equals(" ") || attribute.equals("")){
						context.write(new Text(fulname+"."+"empty"), ONE);
					}else{
						
						if(name.equals("domain") || name.equals("referer")){
//							String result = getDomain(attribute);
							String result = mapTools.extractDomainFromUrl(attribute);
							context.write(new Text(fulname+"-&-"+result), ONE);
						
						}else if(name.equals("creative_type")){
							if(attribute.contains("|")){
								StringTokenizer type = new StringTokenizer(attribute,"\\|");
								while(type.hasMoreTokens())
									context.write(new Text(fulname+"-&-"+type.nextToken()), ONE);
							}else
								context.write(new Text(fulname+"-&-"+attribute), ONE);
//						}else if(name.equals("user_ip")){
//							context.write(new Text(fulname+"-&-"+attribute), ONE);
//							String geoinfo = IpToGeo.getGeo(attribute);
//							context.write(new Text(exchange+".GEO-&-"+geoinfo),ONE);
						}else{
							context.write(new Text(fulname+"-&-"+attribute), ONE);
							}
					}
				}
			}
		}
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for(String exc : Exchanges){
			Map<Integer,String> freq_exc = tmp.get(exc);
			Iterator<Integer> it = freq_exc.keySet().iterator();
			Counter lines = context.getCounter("LINE_NUM",exc);
			while(it.hasNext()){
				i = it.next();
				String name = freq_exc.get(i);
				String fulname = exc+"."+name;
				context.write(new Text("LINES-"+fulname), new IntWritable((int)lines.getValue()));
			}
		}
	}
}
