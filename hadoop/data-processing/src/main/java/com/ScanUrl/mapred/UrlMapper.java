package mapred;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.Config_url;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UrlMapper extends Mapper<LongWritable, Text, Text, Text> {

	Map<String,Integer> LogFormat = null;
	Map<String, Integer> Dimentions = null;
	ArrayList<String> black_list = null;
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ArrayList<String> splited = utils.Spliter.splited(value.toString(),"\t");
		if(splited.size() != 64 && splited.size() != 37 && splited.size() < LogFormat.size()){
			System.out.println("size of format : " + LogFormat.size() + "  size of line : " + splited.size());
			System.out.println("the line : " + value.toString());
			return;
		}
		String url = null, size = null;
		for(String item : Dimentions.keySet()){
			if(item.equals("url"))
				url = splited.get(Dimentions.get("url"));
			if(item.equals("size"))
				size = splited.get(Dimentions.get("size"));
		}
		
		String domain = get_domain(url);
		if(url.length() > 300){
			url = null;
			context.getCounter("black_list", "length_more_then_450").increment(1);
		}
		for(String item : black_list){
			if(domain.startsWith(item)){
				url = null;
				context.getCounter("black_list", "exclude_count").increment(1);
			}
		}
		if(url != null && size != null)
			context.write(new Text(url),new Text(size));
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String path = conf.get("CONFPATH");
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(path),conf);
			Config_url config = Config_url.getInstance();
			config.loadConfig(path, fs);
			Dimentions = config.getDimentionFormat();
			LogFormat = config.getLogFormat();
			black_list = config.getBlack_list();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	public String get_domain(String val) {
		if(val==null || val.trim().equals("")){
			return "unknown";
		}
		String domain = val;
		Pattern p = Pattern.compile("(?<=//)((\\w)+\\.)+\\w+");
		Matcher matcher = p.matcher(val);  
		if(matcher.find()){
			domain = matcher.group();
			if(domain.startsWith("www."))
				domain = domain.substring(4);
		}
		return domain;
	}

}
