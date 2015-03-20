package com.bilin.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class WeekUserProcessor {

    public static void runJob(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WeekUserJobConf(), args);
    }

    public static void main(String[] args) throws Exception {
        if (4 > args.length) {
            System.err.println("Missing required parameter!");
            System.err.println("parameters: input_path output_path logType properties_file_path");
            System.exit(2);
        }
        runJob(args);
    }
}
