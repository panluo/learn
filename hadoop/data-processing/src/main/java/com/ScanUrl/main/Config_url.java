package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Config_url {
	private static Config_url config = null;
	private Map<String, Integer> LogFormat = new HashMap<String, Integer>();
	private Map<String, Integer> Dimention = new HashMap<String, Integer>();
	private ArrayList<String> black_list = new ArrayList<String>();
	
	private Config_url(){};
	
	public static Config_url getInstance(){
		if(config == null)
			return new Config_url();
		else
			return config;
	}
	
	public void loadConfig(String ConfPath, FileSystem fs) throws IllegalArgumentException, IOException{
		FSDataInputStream in = fs.open(new Path(ConfPath));
		Properties pt = new Properties();
		pt.load(in);
		
		int pos = 0;
		StringTokenizer st_log = new StringTokenizer(pt.getProperty("req"),",");
		while(st_log.hasMoreTokens()){
			LogFormat.put(st_log.nextToken(),pos);
			pos++;
		}
		StringTokenizer st_dim = new StringTokenizer(pt.getProperty("dimention"),",");
		while(st_dim.hasMoreTokens()){
			String item = st_dim.nextToken();
			if(LogFormat.containsKey(item)){
				Dimention.put(item, LogFormat.get(item));
			}else{
				System.err.println("ERROR : configuration file Error with wrong dimention");
				System.exit(1);
			}
		}
		StringTokenizer st_bk = new StringTokenizer(pt.getProperty("black_list"),",");
		while(st_bk.hasMoreTokens()){
			black_list.add(st_bk.nextToken());
		}
	}
		
	public Map<String,Integer> getLogFormat(){
		return LogFormat;
	}
	
	public Map<String, Integer> getDimentionFormat(){
		return Dimention;
	}
	
	public ArrayList<String> getBlack_list(){
		return black_list;
	}
}
