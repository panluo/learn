package com.bilin.main;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Config {
    private Map<String, Set<String>> keywordsMap = new HashMap<String, Set<String>>();
    private static Map<String, Integer> proper = new HashMap<String, Integer>();
    private static ArrayList<String> logFormat = new ArrayList<String>();
    private static ArrayList<String> campaigns = new ArrayList<String>();
    private static Map<String,Set<String>> black_list_req = new HashMap<String,Set<String>>();
    private static Map<Integer, Set<String>> black_list = new HashMap<Integer, Set<String>>();
    private static final String COMMA = ",";
    private static final String KEYWORDS = "keywords.";
    private static int url_Pos = -1;
    private static int length = -1;
    //    private static boolean hasBlackList = false;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Config config = null;

    static {
        if (config == null) {
            config = new Config();
        }
    }

    public static Config getInstance() {
        if (config == null) {
            config = new Config();
        }
        return config;
    }

    public long getTimeStamp(String key) {
        String t = key.substring(key.lastIndexOf(".") + 1);
        try {
            return sdf.parse(t.substring(0, t.lastIndexOf("-")) + " " + t.substring(t.lastIndexOf("-") + 1) + ":00:00")
                    .getTime() / 1000;
        } catch (ParseException e) {
            return -1;
        }
    }

    public void loadConfig(String logType, String filePath) {

        FSDataInputStream in;
        Properties props = new Properties();

        Configuration conf = new Configuration();
        try {
            //use this function to get fileSystem for using the s3 service
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            in = fs.open(new Path(filePath));
            try {
                props.load(in);

                StringTokenizer log_format = new StringTokenizer(props.getProperty(logType), Config.COMMA);
                StringTokenizer format = new StringTokenizer(props.getProperty("result_format"), Config.COMMA);
                StringTokenizer cps = new StringTokenizer(props.getProperty("campaigns"), Config.COMMA);
                StringTokenizer blk_list = new StringTokenizer(props.getProperty("black_list_req"),Config.COMMA);
               
                length = 0;
                String item;
                proper.clear();
                while (log_format.hasMoreTokens()) {
                    item = log_format.nextToken();
                    if (item.equals(props.getProperty("url_pos")) && !"-1".equals(props.getProperty("url_pos")))
                        url_Pos = length;
                    proper.put(item, length);
                    length++;
                }
                if (!"-1".equals(props.getProperty("url_pos"))) {
                    Enumeration<?> en = props.propertyNames();
                    while (en.hasMoreElements()) {
                        String key = (String) en.nextElement();
                        if (key.contains(KEYWORDS)) {
                            Set<String> temp = new HashSet<String>();
                            StringTokenizer st = new StringTokenizer(props.getProperty(key), ",");
                            while (st.hasMoreElements()) {
                                temp.add(st.nextToken());
                            }
                            keywordsMap.put(key.substring(key.indexOf(".", 0) + 1,
                                    key.lastIndexOf(".") + 1) + getTimeStamp(key.substring(key.lastIndexOf("" +
                                    ".") + 1)), temp);
                        } else if (key.contains("blacklist.")) {
                            Set<String> temp = new HashSet<String>();
                            StringTokenizer st = new StringTokenizer(props.getProperty(key), ",");
                            while (st.hasMoreElements()) {
                                String str = st.nextToken();
                                if ("null".equals(str)) {
                                    temp = null;
                                    break;
                                } else
                                    temp.add(str);
                            }
                            black_list.put(Integer.parseInt(key.substring(key.indexOf(".", 0) + 1)), temp);
                        }
                    }
//                    System.out.println("---------" + keywordsMap.size() + "+++++" + black_list.size());
                } else {
                    keywordsMap = null;
                    black_list = null;
                }
                logFormat.clear();
                while (format.hasMoreTokens()) {
                    logFormat.add(format.nextToken());
                }
                campaigns.clear();
                while (cps.hasMoreTokens()) {
                    campaigns.add(cps.nextToken());
                }
                black_list_req.clear();
                while (blk_list.hasMoreTokens()) {
                	
                	String domain = blk_list.nextToken();
                    Pattern pattern = Pattern.compile("[^\\.]+(\\.com|\\.net|\\.cn|\\.org|\\.biz|\\.info|\\.cc|\\.tv"
                			+ "|\\.de|\\.fm|\\.eu|\\.es|\\.fi|\\.in|\\.co|\\.ga|\\.me|\\.io)");
                	Matcher matcher = pattern.matcher(domain);
                	String top_domain = null;
                	if(matcher.find())
                		top_domain = matcher.group();
                	else
                		continue;
                	
                	if(black_list_req.containsKey(top_domain))
                		black_list_req.get(top_domain).add(domain);
                	else{
                		Set<String> init = new HashSet<String>();
                		init.add(domain);
                		black_list_req.put(top_domain, init);
                	}
                }
            } finally {
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Set<String>> getKeywordsMap() {
        return keywordsMap;
    }

    public static int getUrl_Pos() {
        return url_Pos;
    }

    public static int getLength() {
        return length;
    }

    public static Map<String, Integer> getLineOfLog() {
        return proper;
    }

    public static ArrayList<String> getFormatLog() {
        return logFormat;
    }

    public static ArrayList<String> getCampaigns() {
        return campaigns;
    }

    public static Map<Integer, Set<String>> getBlack_list() {
        return black_list;
    }
    
    public static Map<String,Set<String>> getblack_list_req(){
    	return black_list_req;
    }
}
