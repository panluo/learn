package com.bilin.job;

import com.bilin.core.WeekUserMapOutValue;
import com.bilin.core.WeekUserRunReduce;
import com.bilin.main.WeekUserConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WeekUserProcessReducer extends Reducer<Text, WeekUserMapOutValue, Text, Text> {

    private WeekUserRunReduce weekUserRunReduce;
//    private MultipleOutputs<Text, Text> mos;
    private String logType;
    private Text result = new Text();
    private MultipleOutputs<Text,Text> mos;

//    private final String SEPERATOR = "-";

    public void loadConfig(Context context) {
        logType = context.getConfiguration().get("logType");
        String filePath = context.getConfiguration().get("property_file_path");
        WeekUserConfig.weekUserConfig.loadConfig(logType,filePath);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    protected void setup(Context context) {
        this.loadConfig(context);
        mos = new MultipleOutputs<Text, Text>(context);
        String clsName = WeekUserConfig.weekUserConfig.getReduceClsName();
        try {
            weekUserRunReduce = (WeekUserRunReduce) Class.forName(clsName).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<WeekUserMapOutValue> mapOutValue,
                          Context context) throws IOException, InterruptedException {
        weekUserRunReduce.setValue(mapOutValue);
        result.set(weekUserRunReduce.run());
        mos.write(key,result,this.getFileName());
    }

    String getFileName(){
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE,-1);
        return "weeklyUserCategorization-" + new SimpleDateFormat("yyyyMMdd").format(c.getTime());
    }
}
