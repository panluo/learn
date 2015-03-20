package com.bilin.job;

//import com.bilin.core.PixelMapOutValue;
import com.bilin.core.PixelRunMap;
import com.bilin.main.PixelConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class PixelLogProcessMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final String LOGTYPE = "logType";

    private static final String PROPERTY_FILE_PATH = "property_file_path";

    private PixelRunMap runmap;

    Map<Text, Text> mapOutKey;

    static enum WrongLog {
        WRONGLOG
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        PixelConfig.pixelConfig.loadConfig(context.getConfiguration().get(LOGTYPE), context.getConfiguration()
                .get(PROPERTY_FILE_PATH));
        try {
            runmap = (PixelRunMap) Class.forName(PixelConfig.pixelConfig.getMapClassName()).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        mapOutKey = runmap.run(value);
        if (null == mapOutKey) {
            context.getCounter(WrongLog.WRONGLOG).increment(1);
            return;
        }
        for (Text resultkey : mapOutKey.keySet()) {
            context.write(resultkey, mapOutKey.get(resultkey));
        }
    }
}
