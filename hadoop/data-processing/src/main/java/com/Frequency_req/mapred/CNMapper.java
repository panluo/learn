package mapred;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import main.Config;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import util.GetDomain;
import util.Spliter;

public class CNMapper implements RunMapper {

	@Override
	public Map<Text, LongWritable> run(Text lineOfLog,
			Map<String, Integer> logformat) {
		Map<String, ArrayList<String>> dimention = Config.getInstance().getDimentionFormat();
		LongWritable ONE = new LongWritable(1);
		ArrayList<String> line = Spliter.spliter(lineOfLog.toString(), "\t");
		
		Map<Text, LongWritable> result = new HashMap<Text, LongWritable>();
		
		if(line.size() != logformat.size() && line.size() != 29){
			System.out.println("number of log field : " + line.size());
			System.out.println("number of format field : " + logformat.size());
			System.out.println("the line : " + lineOfLog.toString());
			return null;
		}
		String exchange = line.get(logformat.get("ad_exchange"));
		if(dimention.containsKey(exchange)){
			result.put(new Text("LINE_NUM_" + exchange), ONE);
			for(String dimention_type : dimention.get(exchange)){
				String dimention_value = line.get(logformat.get(dimention_type));
				String key_in_line = exchange.concat("+=&=+").concat(dimention_type).concat("+=&=+");
				
				if(dimention_value.isEmpty()){
					key_in_line = key_in_line.concat("Empty");
					result.put(new Text(key_in_line),ONE);
				}else{
					result.put(new Text(key_in_line.concat("count")),ONE);
					if(dimention_type.equals("referer")){
						dimention_value = GetDomain.getDomain(dimention_value);
						result.put(new Text(key_in_line.concat(dimention_value)), ONE);
					}else if(dimention_type.equals("domain")){
						dimention_value = GetDomain.getDomain(line.get(logformat.get("url")));
						result.put(new Text(key_in_line.concat(dimention_value)), ONE);
					}else if(dimention_type.equals("creative_type") || dimention_type.equals("site_category")){
					
						if(dimention_value.contains("|")){
							StringTokenizer type = new StringTokenizer(dimention_value,"\\|");
							while(type.hasMoreTokens())
								result.put(new Text(key_in_line.concat(type.nextToken())), ONE);
						}else
							result.put(new Text(key_in_line.concat(dimention_value)), ONE);
					
					}else
						result.put(new Text(key_in_line.concat(dimention_value)), ONE);
				}
				
			}
			ArrayList<String> total_flow = Config.getInstance().getTotalFlow();
			if(total_flow != null){
				for(String item : total_flow){
					String value = line.get(logformat.get(item));
					if(value.trim().isEmpty())
						value = "Empty";
					result.put(new Text("total_flow_" + item + "+=&=+" + value),ONE);
				}
			}
		}else{
			result.put(new Text("Error_In_Exchange"), ONE);
		}
		return result;
	}

}
