package com.bilin.main;

import com.bilin.job.ImpMapper;
import com.bilin.job.ReqExtractUrlMapper;
import com.bilin.job.ReqExtractUrlReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobConf extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
//        FileSystem fs = FileSystem.get(conf);
//        fs.delete(new Path(args[2]), true);
        conf.set("logType", args[3]);
        conf.set("property_file_path", args[4]);
        conf.set("RATE", args[5]);
        conf.set("switch", args[6]);
        Calendar c = Calendar.getInstance();
        String date = "" + c.get(Calendar.YEAR) + (c.get(Calendar.MONTH) + 1) + c.get(Calendar.DAY_OF_MONTH) + c.get(Calendar.HOUR_OF_DAY);
        Job job = Job.getInstance(conf, args[3] + "_extract_url_" + date);

        job.setJarByClass(Processor.class);
        job.setReducerClass(ReqExtractUrlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        FileInputFormat.setInputDirRecursive(job, true);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReqExtractUrlMapper.class);
        try {
            @SuppressWarnings("unused")
			int tmp = Integer.parseInt(args[1]);
        }catch (NumberFormatException e){
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ImpMapper.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        MultipleOutputs.setCountersEnabled(job, true);

        job.waitForCompletion(true);
        
        String datetime,hour;
        Pattern pattern = Pattern.compile("\\d{10}");
        Matcher m = pattern.matcher(args[0]);
        if(m.find()){
        	String day_hour=m.group();
        	hour=day_hour.substring(8);
        	datetime=day_hour.substring(0, 8);
        }else{
        	 String[] time = getOneHoursAgoTime().split(" ");
             datetime = time[0];
             hour = time[1];
        }
        long imp_num = job.getCounters().findCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs","prelytix/imp").getValue();
        long req_num = job.getCounters().findCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs","prelytix/req").getValue();
        long input = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","MAP_INPUT_RECORDS").getValue();
        String num_path = "/user/hadoop/prelytix_num_count/"+ datetime;
//        String num_path = "/home/luo/sample/"+datetime;
        FileSystem fsR = FileSystem.get(new URI(num_path), conf);
        fsR.createNewFile(new Path(num_path));
        FSDataOutputStream fout = fsR.append(new Path(num_path));
//        FSDataOutputStream fout = fsR.create(new Path(num_path));
        BufferedWriter out = null;
        try{
        	out = new BufferedWriter(new OutputStreamWriter(fout,"utf-8"));
        	out.write("date : ".concat(datetime +" "+ hour).concat("\timp count : " + imp_num).concat("\treq count : " + req_num).concat("\tinput count : " + input));
        	out.newLine();
        	out.flush();
        }finally{
        	if(out != null)
        		out.close();
        	fout.close();
        }
        return 0;
    }
    public String getOneHoursAgoTime() {
        String oneHoursAgoTime = "";
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR, 1);
        oneHoursAgoTime = new SimpleDateFormat("yyyyMMdd HH")
                .format(cal.getTime());

        return oneHoursAgoTime;
    }
}
