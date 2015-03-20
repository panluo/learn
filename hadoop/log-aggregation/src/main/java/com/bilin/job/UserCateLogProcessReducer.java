package com.bilin.job;

import com.bilin.core.UserCateMapOutValue;
import com.bilin.core.UserCateRunReduce;
import com.bilin.main.UsesrCateConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class UserCateLogProcessReducer extends Reducer<Text, UserCateMapOutValue, Text, Text> {

    private UserCateRunReduce runReduce;
//    private MultipleOutputs<Text, Text> mos;
    private String logType;
    private Text result = new Text();
    private MultipleOutputs<Text,Text> mos;

//    private final String SEPERATOR = "-";

    public void loadConfig(Context context) {
        logType = context.getConfiguration().get("logType");
        String filePath = context.getConfiguration().get("property_file_path");
        UsesrCateConfig.usesrCateConfig.loadConfig(logType, filePath);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    protected void setup(Context context) {
        this.loadConfig(context);
        mos = new MultipleOutputs<Text, Text>(context);
        String clsName = UsesrCateConfig.usesrCateConfig.getReduceClsName();
        try {
            runReduce = (UserCateRunReduce) Class.forName(clsName).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<UserCateMapOutValue> mapOutValue,
                          Context context) throws IOException, InterruptedException {
        runReduce.setValue(mapOutValue);
        result.set(runReduce.run());
        mos.write(key,result,this.getFileName());
    }

    String getFileName(){
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE,-1);
        return "UserCategorization-" + new SimpleDateFormat("yyyyMMdd").format(c.getTime());
    }
}
