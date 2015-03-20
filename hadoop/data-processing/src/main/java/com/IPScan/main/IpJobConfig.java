package main;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import util.PathGenerator;
import code.IpReduce;
import code.MapOutValue;
import code.WeekdayMap;
import code.WorkdayMap;


public class IpJobConfig extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		PathGenerator gt = new PathGenerator();
		
		conf.set("CONFPATH", args[2]);
		int days = 7;
		if(args.length==5){
			days = Integer.parseInt(args[4]);
		}
		Job job = Job.getInstance(conf,"IP_Scan");
		job.setJarByClass(Process.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(IpReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapOutValue.class);
		job.setNumReduceTasks(1);
		
		String input = args[0];
//		MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class, WeekdayMap.class);
		for(int i=0; i < days; i++){
			if(fs.exists(new Path(input))){
				if(gt.isWeekend(input))
					MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class, WeekdayMap.class);
				else if(gt.isWeekend(input) != null)
					MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class, WorkdayMap.class);
			}
			String newday = gt.newPath(input);
			System.out.println(input);
			if(newday!=null)
				input = newday;
			else
				break;
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        MultipleOutputs.setCountersEnabled(job, true);
		job.addCacheFile(new URI(args[3]));
		job.waitForCompletion(true);
		return 0;
	}

}
