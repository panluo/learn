package utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Config {
	private static Map<String, Integer> proper = new HashMap<String, Integer>();
	private static ArrayList<String> line_item = new ArrayList<String>();
	private static Config config;
	private static String keys = "";
	
	public static Config getInstance(){
		if(config == null){
			config = new Config();
		}
		return config;
	}
	
	public void LoadConf(String path,String type) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(path));
		
		//Useful	
		Properties props = new Properties();
		props.load(in);
		StringTokenizer property = new StringTokenizer(props.getProperty(type+"_properties"),",");
		StringTokenizer line = new StringTokenizer(props.getProperty(type+"_line"),",");
		StringTokenizer useful = new StringTokenizer(props.getProperty(type+"_useful_line"),",");
		int pos=0;
		while(property.hasMoreTokens()){
			proper.put(property.nextToken(), pos++);
		}
		pos=0;
		while(useful.hasMoreTokens()){
			String item = useful.nextToken();
			line_item.add(item);
		}
		while(line.hasMoreTokens()){
			keys = keys.concat(line.nextToken()+"\t");
		}
		
		in.close();
	}
	
	public static Map<String, Integer> getProper(){
		return Config.proper;
	}
	
	public static String getLineName(){
		return Config.keys;
	}
	
	public static ArrayList<String> getItems(){
		return Config.line_item;
	}
}
