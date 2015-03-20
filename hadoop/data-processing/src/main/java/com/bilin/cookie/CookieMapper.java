package cookie;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CookieMapper extends Mapper<LongWritable, Text, Text, MapOutValue> {

	Map<String, Integer> logFormat = null;
	ArrayList<String> str = null;
	ArrayList<String> list = null;
	SimpleDateFormat format = new SimpleDateFormat("YYYY-dd-MM HH:mm:ss");
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, MapOutValue>.Context context)
			throws IOException, InterruptedException {

		str = spliter(value.toString(),"\t");
		if(str.size() != 64 && str.size() != 37 && str.size() != 29){
			System.out.println("filed number of line :" + str.size());
			System.out.println("the line :" + value.toString());
			context.getCounter("Error_Log", "Wrong number of filed").increment(1);
		}else{
			String ip="",geo="",time="",domain="",exchange_user_id="",os="",isDomain;
			if(str.size() < logFormat.size() && str.size() == 29)		//cursor in china
				isDomain = "url"; 
			else
				isDomain = "domain";
			
			for(String item : list){
				
				String val = str.get(logFormat.get(item));
				if(item.equals("user_ip"))
					ip = val;
				else if(item.equals("geo"))
					geo = val;
				else if(item.equals("time"))
					time = format.format(new Date(Long.parseLong(val)*1000));
				else if(item.equals("os"))
					os = val;
				else if(item.equals(isDomain)){
					if(isDomain.equals("url"))
						domain = get_domain(val);
					else
						domain = val;
				}else if(item.equals("exchange_user_id"))
					exchange_user_id = val;
			}
			if(!exchange_user_id.isEmpty()){
				MapOutValue mapoutvalue = new MapOutValue(ip,geo,time,os,domain);
				context.write(new Text(exchange_user_id), mapoutvalue);
			}
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

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, MapOutValue>.Context context)
			throws IOException, InterruptedException {
		String filepath = context.getConfiguration().get("CONF_PATH");
		try {
			Config.getInstance().loadconf(filepath);
			logFormat = Config.getInstance().getLogFormat();
			list = Config.getInstance().getList();
			
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	
	public ArrayList<String> spliter(String line,String separator){
		ArrayList<String> splited = new ArrayList<String>();
		int pos=0;
		int end=0;
		while((end = line.indexOf(separator,pos)) != -1){
			splited.add(line.substring(pos, end));
			pos = end + separator.length();
		}
		splited.add(line.substring(pos));
		return splited;
	}
}
