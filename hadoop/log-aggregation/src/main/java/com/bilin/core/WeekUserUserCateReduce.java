package com.bilin.core;

import com.bilin.main.WeekUserConfig;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class WeekUserUserCateReduce implements WeekUserRunReduce {

    private Iterator<WeekUserMapOutValue> values;
    //site_category,count
    private Map<String, Integer> site_category_count = new HashMap<String, Integer>();
    //page_category,count
    private Map<String, Integer> page_category_count = new HashMap<String, Integer>();
    private long total;
    private static final String DELIM = "|";
    private static final String COMMA = ",";
    private static final String NULLRES = "null";
    private StringBuffer strbuf = new StringBuffer();

    public String run() {
        total = 0;
        int min_time_stamp = Integer.MAX_VALUE;
        int max_time_stamp = Integer.MIN_VALUE;
        site_category_count.clear();
        page_category_count.clear();
        while (values.hasNext()) {
            WeekUserMapOutValue mvalue = values.next();
            StringTokenizer site_categories = new StringTokenizer(mvalue.getSite_category(), DELIM);
            while (site_categories.hasMoreElements()) {
                String site_cate = site_categories.nextToken();
                if (site_category_count.containsKey(site_cate)) {
                    site_category_count.put(site_cate, site_category_count.get(site_cate) + Integer.parseInt(site_categories
                            .nextToken()));
                } else
                    site_category_count.put(site_cate, Integer.parseInt(site_categories.nextToken()));
            }
            StringTokenizer page_categories = new StringTokenizer(mvalue.getPage_category(), DELIM);
            while (page_categories.hasMoreElements()) {
                String page_cate = page_categories.nextToken();
                if (page_category_count.containsKey(page_cate)) {
                    page_category_count.put(page_cate, page_category_count.get(page_cate) + Integer.parseInt
                            (page_categories.nextToken()));
                } else
                    page_category_count.put(page_cate, Integer.parseInt(page_categories.nextToken()));
            }
            total += Integer.parseInt(mvalue.getTotal());
            int time_stamp_max = Integer.parseInt(mvalue.getMax_time_stamp());
            int time_stamp_min = Integer.parseInt(mvalue.getMin_time_stamp());
            if (time_stamp_max > max_time_stamp)
                max_time_stamp = time_stamp_max;
            if (time_stamp_min < min_time_stamp)
                min_time_stamp = time_stamp_min;
        }

        float average_time = ((float) max_time_stamp - min_time_stamp) / total;
        strbuf.setLength(0);
        strbuf.append(new DecimalFormat("#.####").format(average_time));

        strbuf.append(COMMA);
        for (String site_category : site_category_count.keySet()) {
            if (WeekUserConfig.weekUserConfig.getSite_threshold() < ((float) site_category_count.get(site_category) / total)) {
                strbuf.append(site_category);
                strbuf.append(DELIM);
            }
        }
        if (strbuf.lastIndexOf(DELIM) != -1)
            strbuf = strbuf.replace(strbuf.lastIndexOf(DELIM), strbuf.length(), COMMA);
        else
            strbuf.append(NULLRES + COMMA);
        for (String page_category : page_category_count.keySet()) {
            if (WeekUserConfig.weekUserConfig.getPage_threshold() < ((float) page_category_count.get(page_category) / total)) {
                strbuf.append(page_category);
                strbuf.append(DELIM);
            }
        }
        if (strbuf.lastIndexOf(",") < strbuf.lastIndexOf(DELIM))
            return strbuf.substring(0, strbuf.length() - 1);
        else
            return strbuf.append(NULLRES).toString();
//        if (strbuf.lastIndexOf(COMMA) < strbuf.lastIndexOf(DELIM)) {
//            strbuf.replace(strbuf.lastIndexOf(DELIM), strbuf.length(), COMMA);
//            strbuf.append(total);
//        } else {
//            strbuf.append(NULLRES);
//            strbuf.append(COMMA);
//            strbuf.append(total);
//        }
//        return strbuf.toString();
    }

    @Override
    public void setValue(Iterable<WeekUserMapOutValue> mapOutValue) {
        Iterator<WeekUserMapOutValue> ite = mapOutValue.iterator();
        values = ite;
    }
}
