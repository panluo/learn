package com.bilin.main;

import com.bilin.core.WeekUserMapOutValue;
import com.bilin.job.WeekUserProcessMapper;
import com.bilin.job.WeekUserProcessReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import java.util.Calendar;

public class WeekUserJobConf extends Configured implements Tool {

    public static final String PROPERTY_FILE_PATH = "property_file_path";
    public static final String LOGTYPE = "logType";

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]), true);          //

        conf.set(LOGTYPE, args[2]);
        conf.set(PROPERTY_FILE_PATH, args[3]);

        conf.set("mapreduce.output.textoutputformat.separator",",");

        Calendar c = Calendar.getInstance();

        Job job = Job.getInstance(conf, args[2] + "_" + c.get(Calendar.YEAR)
                + (c.get(Calendar.MONTH) + 1) + c.get(Calendar.DAY_OF_MONTH)
                + c.get(Calendar.HOUR_OF_DAY));
        job.setJarByClass(WeekUserProcessor.class);
        job.setMapperClass(WeekUserProcessMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WeekUserMapOutValue.class);
        job.setReducerClass(WeekUserProcessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(7);						//
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.setCountersEnabled(job,true);

        int exitcode = job.waitForCompletion(true) ? 0 : 1;
        return exitcode;
    }
}
