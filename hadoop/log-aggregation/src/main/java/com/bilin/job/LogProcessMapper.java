package com.bilin.job;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bilin.core.MapOutValue;
import com.bilin.core.RunMap;
import com.bilin.ip.IpToGeo;
import com.bilin.main.Config;

public class LogProcessMapper extends Mapper<NullWritable, Text, Text, MapOutValue> {

    public static final String LOGTYPE = "logType";

    public static final String PROPERTY_FILE_PATH = "property_file_path";

    public RunMap runmap;

    Map<Text, MapOutValue> mapOutKey;

    static enum WrongLog {
        WRONGLOG
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Config.getInstance().loadConfig(context.getConfiguration().get(LOGTYPE), context.getConfiguration()
                .get(PROPERTY_FILE_PATH));
        
        URI[] localCacheFile = context.getCacheFiles();                //get ip to geo code file china.csv and loading
        FileSystem fs = FileSystem.get(localCacheFile[0], new Configuration());
        IpToGeo.loadGeoFile(localCacheFile[0].toString(),fs);
        try {
            runmap = (RunMap) Class.forName(Config.getInstance().getMapClassName()).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
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
