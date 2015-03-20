package com.bilin.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.bilin.main.Config;
import com.bilin.utils.DateTransformer;
import com.bilin.utils.Spliter;

public class ImpMapper extends Mapper<LongWritable, Text, Text, Text> {
	ArrayList<String> format_log = null;
	Map<String,Integer> lineOfLog = null;
	ArrayList<String> format = null;
	ArrayList<String> campaign = null;
	MultipleOutputs<Text,Text> multipleOutputs;
	private static enum WrongLog {
        WRONGLOG, OUTBOUND
	}
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		ArrayList<String> str = Spliter.splits(value.toString(), "\t");
//		if(str.size() < lineOfLog.size()){
		if(str.size() < 61){
			System.out.println("line length: "+str.size() + "  proper length: " + lineOfLog.size());
			System.out.println(value.toString());
			context.getCounter(WrongLog.WRONGLOG).increment(1);
		}else{
			String campaign_id_log=str.get(lineOfLog.get("campaign_id"));
			int cp = 0;
			for(cp=0;cp < campaign.size();cp++){
				if(campaign.get(cp).equals(campaign_id_log))
					break;
			}
			if(cp != campaign.size()){
				format_log = new ArrayList<String>();
				format_log.clear();
				
				for(int i=0; i < format.size()-1; i++){
					format_log.add("");
				}
				int i=0;
				for(String item : format){
					if(lineOfLog.containsKey(item)){
						format_log.set(i, str.get(lineOfLog.get(item)));
					}
					i++;
				}
			
				String ESTTime = DateTransformer.UTCToEST(str.get(lineOfLog.get("date_time")), str.get(lineOfLog.get("time_zone")));
				format_log.set(format.indexOf("date_time"), ESTTime);
				// geo
				String geo_info = str.get(lineOfLog.get("geo"));
				ArrayList<String> geo = Spliter.splits(geo_info,"|");
				if(geo.size()==4){
					for(int j=0; j<4; j++){
						format_log.set(format.indexOf("user_country")+j, geo.get(j));
					}
				}
			
				format_log.set(format.indexOf("ip"), format_log.get(format.indexOf("user_ip")));
				format_log.set(format.indexOf("country"), format_log.get(format.indexOf("user_country")));
				format_log.set(format.indexOf("region"), format_log.get(format.indexOf("user_region")));
		
				String flash = str.get(lineOfLog.get("is_flash_allowed"));
				if(flash.equalsIgnoreCase("true"))
					format_log.set(format.indexOf("with_flash"), "yes");
				else if(flash.equalsIgnoreCase("false"))
					format_log.set(format.indexOf("with_flash"), "yes");
				else
					format_log.set(format.indexOf("with_flash"),flash);
				
				String uuid = str.get(lineOfLog.get("uuid"));
				if(uuid.isEmpty())
					format_log.set(format.indexOf("with_sync"), "0");
				else
					format_log.set(format.indexOf("with_sync"), "1");
				
//				format_log.set(format.indexOf("currency"),"USD");
			
				String line="";
				for(String val : format_log){
					line = line.concat(val.concat("\t"));
				}
				multipleOutputs.write(new Text(line.substring(0,line.length()-1)), new Text("USD"),"prelytix/imp");
//				context.write(new Text(line.substring(0,line.length()-1)), NullWritable.get());
			}else
				context.getCounter(WrongLog.OUTBOUND).increment(1);
		}
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
	protected void setup(
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
		Config.getInstance().loadConfig("imp",context.getConfiguration().get("property_file_path"));
		format = Config.getFormatLog();
		lineOfLog = Config.getLineOfLog();
		campaign = Config.getCampaigns();
		
		String line_file="";
		for(String item : format)
			line_file = line_file.concat(item.concat("\t"));
		
		multipleOutputs.write(new Text(line_file.substring(0, line_file.length()-10)), new Text("currency"),"prelytix/imp");
//		context.write(new Text(line_file.substring(0, line_file.length()-1)), NullWritable.get());
	}
}
