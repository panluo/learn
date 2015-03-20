package com.bilin.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Processor {

//    public static String logType;
//
//    public static void loadConfiguration(String[] args) {
//    }
//
//    public static void configuareJob() {
//
//    }

    public static void runJob(String[] args) throws Exception {
    	if(args[2].equals("req"))
    		ToolRunner.run(new Configuration(), new FreqConf(),args);
    	else
    		ToolRunner.run(new Configuration(), new JobConf(), args);
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
