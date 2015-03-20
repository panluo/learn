package com.bilin.job;

import com.bilin.core.UserCateMapOutValue;
import com.bilin.core.UserCateRunMap;
import com.bilin.main.UsesrCateConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

//import org.apache.hadoop.io.NullWritable;

public class UserCateLogProcessMapper extends Mapper<LongWritable, Text, Text, UserCateMapOutValue> {

    public static final String LOGTYPE = "logType";

    public static final String PROPERTY_FILE_PATH = "property_file_path";

    public UserCateRunMap runmap;

    Map<Text, UserCateMapOutValue> mapOutKey;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        UsesrCateConfig.usesrCateConfig.loadConfig(context.getConfiguration().get(LOGTYPE), context.getConfiguration()
                .get(PROPERTY_FILE_PATH));
        try {
            runmap = (UserCateRunMap) Class.forName(UsesrCateConfig.usesrCateConfig.getMapClassName()).newInstance();
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
