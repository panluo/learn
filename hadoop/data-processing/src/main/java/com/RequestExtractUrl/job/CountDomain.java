package com.bilin.job;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bilin.main.Config;
import com.bilin.utils.Spliter;

public class CountDomain {
    public static class CountDomainMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private ArrayList<String> splits = new ArrayList<String>();
        private static IntWritable ONE=new IntWritable(1);

        public void loadConfig(Context context) {
            String logType = context.getConfiguration().get("logType");
            String filePath = context.getConfiguration().get("property_file_path");
            Config.getInstance().loadConfig(logType, filePath);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.loadConfig(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            splits = Spliter.splits(value.toString(),"\t");
            context.write(new Text(Spliter.extractDomainFromUrl(splits.get(Config.getUrl_Pos()))), ONE);
        }
    }

    public static class CountDomainReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (4 > args.length) {
            System.err.println("Missing required parameter!");
            System.err.println("parameters: input_path output_path logType properties_file_path");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.set("logType", args[2]);
        conf.set("property_file_path", args[3]);
        Job job = Job.getInstance(conf, "count domains");
        job.setMapperClass(CountDomainMapper.class);
        job.setCombinerClass(CountDomainReducer.class);
        job.setReducerClass(CountDomainReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
