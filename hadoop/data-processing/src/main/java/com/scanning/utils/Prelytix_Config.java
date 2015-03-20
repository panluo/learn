package utils;

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

public class Prelytix_Config {
	static Prelytix_Config config;
	private static String sep = ","; 
	private static Map<String, Integer> lineOfLog = new HashMap<String,Integer>();
	private static ArrayList<String> logFormat = new ArrayList<String>();
	
	public static Prelytix_Config getInstance(){
		if(config == null){
			config = new Prelytix_Config();
		}
		return config;
	}
	
	public void loadConf(String path, String type) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(path),conf);
		FSDataInputStream in = fs.open(new Path(path));
		Properties props = new Properties();
		props.load(in);
		StringTokenizer log_type = new StringTokenizer(props.getProperty("prelytix_"+type),sep);
		StringTokenizer format = new StringTokenizer(props.getProperty("result_format"),sep);
		int pos = 0;
		lineOfLog.clear();
		while(log_type.hasMoreTokens()){
			lineOfLog.put(log_type.nextToken(),pos++);
		}
		logFormat.clear();
		while(format.hasMoreTokens()){
			logFormat.add(format.nextToken());
		}
	}
	
	public static Map<String,Integer> getLineOfLog(){
		return Prelytix_Config.lineOfLog;
	}
	
	public static ArrayList<String> getFormatLog(){
		return Prelytix_Config.logFormat;
	}
}
