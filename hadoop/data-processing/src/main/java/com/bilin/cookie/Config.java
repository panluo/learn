package cookie;

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

public class Config {
	public static Config conf;
	public static Map<String,Integer> logformat = new HashMap<String,Integer>();
	public static ArrayList<String> list = new ArrayList<String>();

	public static Config getInstance(){
		
		if(conf == null){
			conf = new Config();
		}
		return conf;
	}
	
	public void loadconf(String filepath) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(filepath),conf);
		FSDataInputStream in = fs.open(new Path(filepath));
		Properties props = new Properties();
		props.load(in);
		
		StringTokenizer req = new StringTokenizer(props.getProperty("req"),",");
		StringTokenizer format = new StringTokenizer(props.getProperty("format"),",");
		
		int pos=0;
		while(req.hasMoreTokens()){
			logformat.put(req.nextToken(),pos);
			pos++;
		}
		
		while(format.hasMoreTokens())
			list.add(format.nextToken());
	}
	
	public Map<String,Integer> getLogFormat(){
		return logformat;
	}
	
	public ArrayList<String> getList(){
		return list;
	}
}
