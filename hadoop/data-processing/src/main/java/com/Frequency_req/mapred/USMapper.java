package mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import util.GetDomain;
import util.Spliter;
import main.Config;

public class USMapper implements RunMapper{
	private LongWritable ONE = new LongWritable(1);
	
	@Override
	public Map<Text, LongWritable> run(Text lineOfLog,
			Map<String,Integer> logformat) {
		
		Map<String,ArrayList<String>> dimention = Config.getInstance().getDimentionFormat();
		Map<Text, LongWritable> result = new HashMap<Text, LongWritable>();
		
		ArrayList<String> line = Spliter.spliter(lineOfLog.toString(),"\t");
		if(line.size() != logformat.size() && line.size() != 64){
			System.out.println("number of log field : " + line.size());
			System.out.println("number of format field : " + logformat.size());
			System.out.println("the line : " + lineOfLog.toString());
			return null;
		}
		
		
		String key = line.get(logformat.get("ad_exchange"));
		
		if(dimention.containsKey(key)){
			String ssp = line.get(logformat.get("ssp"));
			if(ssp.isEmpty()){
				result.put(new Text("Error_In_SSP"), ONE);
				System.out.println(lineOfLog.toString());
				return result;
			}else
				result.put(new Text("ssp_num_count_" + ssp), ONE);
			
			String dimention_value, key_in_line;
			for(String dimention_type : dimention.get(key)){
				dimention_value = line.get(logformat.get(dimention_type));
				key_in_line = ssp.concat("+=&=+").concat(dimention_type).concat("+=&=+");
				
				if(dimention_value.isEmpty()){
					key_in_line = key_in_line.concat("Empty");
					result.put(new Text(key_in_line),ONE);
				}else if(dimention_type.equals("referer") || dimention_type.equals("domain")){
					dimention_value = GetDomain.getDomain(dimention_value);
					key_in_line = key_in_line.concat(dimention_value);
					result.put(new Text(key_in_line), ONE);
				}else if(dimention_type.equals("creative_type") || dimention_type.equals("site_category")){
					
					if(dimention_value.contains("|")){
						StringTokenizer type = new StringTokenizer(dimention_value,"\\|");
						while(type.hasMoreTokens())
							result.put(new Text(key_in_line.concat(type.nextToken())), ONE);
					}else
						result.put(new Text(key_in_line.concat(dimention_value)), ONE);
					
				}else if(dimention_type.equals("geo")){
					String city;
					if(dimention_value.contains("|"))
						city = dimention_value.substring(0, dimention_value.lastIndexOf("|"));
					else
						city = dimention_value;
					
					result.put(new Text(key_in_line.concat(city)), ONE);
				}else
					result.put(new Text(key_in_line.concat(dimention_value)), ONE);
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
			result.put(new Text("LINE_NUM_" + key), ONE);
		}else{
			result.put(new Text("Error_In_Exchange"), ONE);
			System.out.println(lineOfLog.toString());
		}
		return result;
	}
}
