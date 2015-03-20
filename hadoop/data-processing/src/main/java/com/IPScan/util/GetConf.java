package util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetConf {
	private static GetConf conf = null;
	Map<String,Float> proper = new HashMap<String,Float>();
	
	static {
		if(conf == null)
			conf = new GetConf();
	}
	
	public static GetConf getinstance(){
		if(conf == null)
			conf = new GetConf();
		return conf;
	}
	
	public void LoadConf(String path) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(path));
		Properties ps = new Properties();
		ps.load(in);
		String[] list = {"X","Y","A","B","C","D","N","T"};
		for(String tmp : list){
			proper.put(tmp,Float.parseFloat(ps.getProperty(tmp)));
			System.out.println(tmp+" "+ps.getProperty(tmp));
		}
	}
	
	public Map<String,Float> getProper(){
		return proper;
	}
}
