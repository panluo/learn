package com.bilin.job;

import com.bilin.main.Config;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.bilin.utils.DateTransformer;
import com.bilin.utils.Spliter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ReqExtractUrlMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ArrayList<String> str = new ArrayList<String>();
    private Map<String,Set<String>> black_list_req = null;
    private Set<String> keys = null;
    private Map<String, Integer> lineOfLog = null;
    private ArrayList<String> format = null;
    private MultipleOutputs<Text, Text> multipleOutputs;
    private String swtich;
    private static enum WrongLog {
        WRONGLOG, OUTBOUND, BLACKLIST, EMPTY
    }

    public void loadConfig(Context context) {
        String logType = context.getConfiguration().get("logType");
        String filePath = context.getConfiguration().get("property_file_path");
        Config.getInstance().loadConfig(logType, filePath);
        format = Config.getFormatLog();
        lineOfLog = Config.getLineOfLog();
        black_list_req = Config.getblack_list_req();
        keys = black_list_req.keySet();
        swtich = context.getConfiguration().get("switch");
    }
    
    public boolean is_blacked(String domain){
    	
//    	Pattern p = Pattern.compile("[^.]*?.(com|cn|net|org|biz|info|cc|tv)",Pattern.CASE_INSENSITIVE);
    	Pattern pattern = Pattern.compile("[^\\.]+(\\.com|\\.net|\\.cn|\\.org|\\.biz|\\.info|\\.cc|\\.tv"
    			+ "|\\.de|\\.fm|\\.eu|\\.es|\\.fi|\\.in|\\.co|\\.ga|\\.me|\\.io)");
    	Matcher matcher = pattern.matcher(domain);
    	while(matcher.find()){
    		String top_domain = matcher.group();
    		if(keys.contains(top_domain))
    			if(black_list_req.get(top_domain).contains(top_domain))
    				return true;
    			else
    				return black_list_req.get(top_domain).contains(domain);
    	}
    	return false;
    }
    

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.loadConfig(context);
        multipleOutputs = new MultipleOutputs<Text, Text>(context);

        String line_file = "";
        for (String item : format)
            line_file = line_file.concat(item.concat("\t"));

        multipleOutputs.write(new Text(line_file.substring(0, line_file.length() - 10)), new Text("currency"), "prelytix/req");
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        str = Spliter.splits(value.toString(), "\t");
//        if (str.size() < Config.getLength()) {
        if (str.size() < 64){
            System.out.println(str.size());
            context.getCounter(WrongLog.WRONGLOG).increment(1);
            return;
        }

        //====================================================
        if (Integer.parseInt(swtich) != 1) {
            int rate = Integer.parseInt(context.getConfiguration().get("RATE"));
            int number = new Random().nextInt(100) + 1;

            int length = str.get(lineOfLog.get("url")).length();
            if (number <= rate && length <= 250){
            	if(str.get(lineOfLog.get("ad_exchange")).isEmpty() && str.get(lineOfLog.get("size")).isEmpty()
            			&& str.get(lineOfLog.get("domain")).isEmpty() && str.get(lineOfLog.get("os")).isEmpty()){
            		context.getCounter(WrongLog.EMPTY).increment(1);
            	}else if(!is_blacked(str.get(lineOfLog.get("domain")))) {

            		ArrayList<String> format_log = new ArrayList<String>();
            		format_log.clear();
            		for (int i = 0; i < format.size() - 1; i++) {
            			format_log.add("");
            		}
            		int i = 0;
            		for (String item : format) {
            			if (lineOfLog.containsKey(item)) {
            				format_log.set(i, str.get(lineOfLog.get(item)));
            			}
            			i++;
            		}

            		//time
            		String ESTTime = DateTransformer.UTCToEST(str.get(lineOfLog.get("date_time")), str.get(lineOfLog.get("time_zone")));
            		format_log.set(format.indexOf("date_time"), ESTTime);
            		// geo
            		ArrayList<String> geo = Spliter.splits(str.get(lineOfLog.get("geo")), "|");
            		if (geo.size() == 4) {
            			for (int j = 0; j < 4; j++) {
            				format_log.set(format.indexOf("user_country") + j, geo.get(j));
            			}
            		}
            		format_log.set(format.indexOf("ip"), format_log.get(format.indexOf("user_ip")));
            		format_log.set(format.indexOf("country"), format_log.get(format.indexOf("user_country")));
            		format_log.set(format.indexOf("region"), format_log.get(format.indexOf("user_region")));

            		// no flash
            		String flash = str.get(lineOfLog.get("is_flash_allowed"));
                	if (flash.equalsIgnoreCase("true"))
                		format_log.set(format.indexOf("with_flash"), "yes");
                	else if (flash.equalsIgnoreCase("false"))
                		format_log.set(format.indexOf("with_flash"), "no");
                	else
                		format_log.set(format.indexOf("with_flash"), flash);

                	String uuid = str.get(lineOfLog.get("uuid"));
                	if (uuid.isEmpty())
                		format_log.set(format.indexOf("with_sync"), "0");
                	else
                		format_log.set(format.indexOf("with_sync"), "1");


                	String line = "";
                	for (String val : format_log) {
                		line = line.concat(val.concat("\t"));
                	}
                	multipleOutputs.write(new Text(line.substring(0, line.length() - 1)), new Text("USD"),
                        "prelytix/req");
            	} else{
            		context.getCounter(WrongLog.BLACKLIST).increment(1);
            	}
            } else 
            	context.getCounter(WrongLog.OUTBOUND).increment(1);
        }
        //=============================================================

        if (2 == Integer.parseInt(swtich))
            return;
        String url = str.get(Config.getUrl_Pos());

        if (url.length() > 256 || 0 >= url.length()) {
            context.getCounter(WrongLog.OUTBOUND).increment(1);
            return;
        }
        boolean contains;
        for (String k : Config.config.getKeywordsMap().keySet()) {
            if (null != Config.getBlack_list().get(Integer.parseInt(k.substring(0,k.indexOf("."))))) {
                for (String next_blank_key : Config.getBlack_list().get(Integer.parseInt(k.substring(0,k.indexOf("."))))) {
                    if (url.equals(next_blank_key) || url.contains(next_blank_key)) {
                        context.getCounter(WrongLog.BLACKLIST).increment(1);
                        return;
                    }
                }
            }

            for (String next_key_word : Config.config.getKeywordsMap().get(k)) {
                String[] arr = next_key_word.split(" ");
                contains = false;
                for (String anArr : arr) {
//                    String patter = "[^A-Za-z]+" + anArr.toLowerCase() + "[^A-Za-z]+";
//                    Pattern p = Pattern.compile(patter, Pattern.UNICODE_CASE);
//                    Matcher m = p.matcher(url);
                    if (!url.contains(anArr.toLowerCase())) {
//                    if (!m.find()) {
                        contains = false;
                        break;
                    } else
                        contains = true;
                }
                if (contains) {
                    context.write(new Text(k + "." + url), new Text("1"));
                }
            }
        }

    }

    @Override
    protected void cleanup(
            Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
