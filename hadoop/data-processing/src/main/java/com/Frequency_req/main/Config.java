package main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import util.Spliter;

public class Config {
	private static Config config = null;
	private Map<String,Integer> lines = new HashMap<String,Integer>();
	private ArrayList<String> exchanges = new ArrayList<String>();
	private Map<String,ArrayList<String>> exchanges_dimention = new HashMap<String,ArrayList<String>>();
	private ArrayList<String> item_total = new ArrayList<String>();
	
	private Config(){}
//	static {
//		config = new Config();
//	}
	
	public static Config getInstance(){
		if(config != null)
			return config;
		else{
			config = new Config();
			return config;
		}
	}
	
	public void loadConfig(String filename) throws IllegalArgumentException, IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(filename),conf);
		FSDataInputStream in = fs.open(new Path(filename));
		Properties pt = new Properties();
		pt.load(in);
		
		StringTokenizer logformat = new StringTokenizer(pt.getProperty("req"),",");
		int pos = 0;
		while(logformat.hasMoreTokens()){
			lines.put(logformat.nextToken(), pos);
			pos++;
		}
		
		StringTokenizer exs = new StringTokenizer(pt.getProperty("exchanges"),",");
		StringTokenizer log_dimention;
		while(exs.hasMoreTokens()){
			String exchange = exs.nextToken();
			exchanges.add(exchange);
			log_dimention = new StringTokenizer(pt.getProperty("exchanges_" + exchange),",");
			ArrayList<String> list = new ArrayList<String>();
			
			while(log_dimention.hasMoreTokens()){
				list.add(log_dimention.nextToken());
			}
			
			exchanges_dimention.put(exchange, list);
		}
		String total_flow = pt.getProperty("total_flow");
		if(total_flow.equalsIgnoreCase("empty") || total_flow.equals("")){
			item_total = null;
		}else
			item_total = Spliter.spliter(total_flow,",");
		
	}

	
	public Map<String,Integer> getLogFormat(){
		return lines;
	}
	
	public Map<String,ArrayList<String>> getDimentionFormat(){
		return exchanges_dimention;
	}
	
	public ArrayList<String> getExchanges(){
		return exchanges;
	}
	
	public String getMapperClass(){
		return getClassName(true);
	}
	
	public String getReducerClass(){
		return getClassName(false);
	}
	
	private String getClassName(boolean isMapper){
		ArrayList<String> exc = getExchanges();
		boolean isBidswitch = exc.contains("bidswitch");
		if(isBidswitch == true && isMapper == true){
			return "mapred.USMapper";
		}else if(isBidswitch == false && isMapper == true){
			return "mapred.CNMapper";
		}else if(isBidswitch == true && isMapper == false){
			return "mapred.USReducer";
		}else
			return "mapred.CNReducer";
	}
	
	public ArrayList<String> getTotalFlow(){
		return item_total;
	}
}
