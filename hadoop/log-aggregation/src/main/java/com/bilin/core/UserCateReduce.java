package com.bilin.core;

import com.bilin.main.UsesrCateConfig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class UserCateReduce implements UserCateRunReduce {

    private Iterator<UserCateMapOutValue> values;
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
            UserCateMapOutValue mvalue = values.next();
            if (site_category_count.containsKey(mvalue.getSite_category())) {
                site_category_count.put(mvalue.getSite_category(), site_category_count.get(mvalue.getSite_category()) +
                        1);
            } else
                site_category_count.put(mvalue.getSite_category(), 1);
            if (page_category_count.containsKey(mvalue.getPage_category())) {
                page_category_count.put(mvalue.getPage_category(), page_category_count.get(mvalue.getPage_category())
                        + 1);
            } else
                page_category_count.put(mvalue.getPage_category(), 1);
            total++;
            int time_stamp = Integer.parseInt(mvalue.getTime_stamp());
            if (time_stamp > max_time_stamp)
                max_time_stamp = time_stamp;
            if (time_stamp < min_time_stamp)
                min_time_stamp = time_stamp;
        }

        strbuf.setLength(0);
//        float average_time = ((float) max_time_stamp - min_time_stamp) / total;
//        strbuf.append(new DecimalFormat("#.####").format(average_time));
//        print max time stamp and min time stamp
        strbuf.append(max_time_stamp);
        strbuf.append(COMMA + min_time_stamp);

        strbuf.append(COMMA);
        for (String site_category : site_category_count.keySet()) {
            if (UsesrCateConfig.usesrCateConfig.getSite_threshold() < ((float) site_category_count.get(site_category) / total)) {
                strbuf.append(site_category);
                strbuf.append(DELIM);
                //print site category counts
                strbuf.append(site_category_count.get(site_category));
                strbuf.append(DELIM);
            }
        }
        if (strbuf.lastIndexOf(DELIM) != -1)
            strbuf = strbuf.replace(strbuf.lastIndexOf(DELIM), strbuf.length(), COMMA);
        else
            strbuf.append(NULLRES + DELIM + 0 + COMMA);
        for (String page_category : page_category_count.keySet()) {
            if (UsesrCateConfig.usesrCateConfig.getPage_threshold() < ((float) page_category_count.get(page_category) / total)) {
                strbuf.append(page_category);
                strbuf.append(DELIM);
                //print page category counts
                strbuf.append(page_category_count.get(page_category));
                strbuf.append(DELIM);
            }
        }
//        if (strbuf.lastIndexOf(",") < strbuf.lastIndexOf(DELIM))
//            return strbuf.substring(0, strbuf.length() - 1);
//        else
//            return strbuf.append(NULLRES).toString();
        if (strbuf.lastIndexOf(COMMA) < strbuf.lastIndexOf(DELIM)) {
            strbuf.replace(strbuf.lastIndexOf(DELIM), strbuf.length(), COMMA);
            strbuf.append(total);
        } else {
            strbuf.append(NULLRES);
            strbuf.append(COMMA);
            strbuf.append(total);
        }
        return strbuf.toString();
    }

    @Override
    public void setValue(Iterable<UserCateMapOutValue> mapOutValue) {
        Iterator<UserCateMapOutValue> ite = mapOutValue.iterator();
        values = ite;
    }
}
