package com.bilin.job;

import com.bilin.core.WeekUserMapOutValue;
import com.bilin.core.WeekUserRunMap;
import com.bilin.main.WeekUserConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

//import org.apache.hadoop.io.NullWritable;

public class WeekUserProcessMapper extends Mapper<LongWritable, Text, Text, WeekUserMapOutValue> {

    public static final String PROPERTY_FILE_PATH = "property_file_path";
    public static final String LOGTYPE = "logType";

    public WeekUserRunMap runmap;

    Map<Text, WeekUserMapOutValue> mapOutKey;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        WeekUserConfig.weekUserConfig.loadConfig(context.getConfiguration().get(LOGTYPE), context.getConfiguration().get
                (PROPERTY_FILE_PATH));
        try {
            runmap = (WeekUserRunMap) Class.forName(WeekUserConfig.weekUserConfig.getMapClassName()).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        mapOutKey = runmap.run(value);
        if (null == mapOutKey) {
            System.out.println(value.toString());
            return;
        }
        for (Text resultkey : mapOutKey.keySet()) {
            context.write(resultkey, mapOutKey.get(resultkey));
        }
    }
}
