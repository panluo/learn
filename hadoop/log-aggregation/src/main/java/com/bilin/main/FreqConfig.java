package com.bilin.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FreqConfig {
	private static String REQ = "req";
	private static String COMMANT = ",";
	private static Map<String, Integer> logFormat = new HashMap<String, Integer>();
	private static FreqConfig config;
	private static Map<String, Map<Integer,String>> tmp = new HashMap<String, Map<Integer,String>>();
	private static Set<String> exchanges = new HashSet<String>();
	//private String logType;
	/*
	 * 初始化方法
	 * 已经被初始化过的对象将不会再次初始化
	 */
	public static FreqConfig getInstance(){
		if(config == null){
			config = new FreqConfig();
		}
		return config;
	}
	/*
	 * 加载配置文件
	 * 配置文件为两行，一行为日志中所有字段，以获取字段在日志中的相对位置
	 * 第二行为所需要统计字段 
	 */
	public void loadConfig(String filePath) throws Exception{
		Configuration conf = new Configuration();
		try{
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(filePath));
			Properties props = new Properties();
			props.load(in);
			StringTokenizer logformat = new StringTokenizer(props.getProperty(REQ), COMMANT);
			StringTokenizer excs = new StringTokenizer(props.getProperty("exchanges"),COMMANT);
			int pos = 0;
			while(logformat.hasMoreTokens()){
				logFormat.put(logformat.nextToken(), pos);
				pos++;
			}
			
			while(excs.hasMoreTokens()){
				exchanges.add(excs.nextToken());
			}
			for(String ex : exchanges){
				String name = "exchanges_"+ex;
				StringTokenizer freq = new StringTokenizer(props.getProperty(name),COMMANT);
				Map<Integer,String> FreqMap = new HashMap<Integer, String>();
				while(freq.hasMoreTokens()){
					String at = freq.nextToken();
					FreqMap.put(logFormat.get(at), at);
				}
//				if(!FreqMap.isEmpty())
				tmp.put(ex,FreqMap);
			}
		}catch(IOException e){
			e.printStackTrace();
		};
	}
	
	/*
	 * 获取与日志格式对应的字段位置信息
	 */
	public Map<String, Integer> getLogFormatMap() {
		return FreqConfig.logFormat;
	}
	/*
	 * 获取所需字段的位置信息
	 */
	public Map<Integer, String> getFreqFormat(String Exchange){
		return FreqConfig.tmp.get(Exchange);
	}
	public Set<String> getExchanges(){
		return exchanges;
	}
	public Map<String, Map<Integer, String>> getFreqMap(){
		return tmp; 
	}
}