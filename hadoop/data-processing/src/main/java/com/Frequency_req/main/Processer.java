package main;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import job.ProcessMapper;
import job.ProcessReducer;
import mapred.SortMapper;
import mapred.SortReducer;
import mapred.SortWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Processer extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		if(args.length < 3){
			System.out.println("usage <input> <output> <conf> <datetime>");
			System.exit(0);
		}
		
		ToolRunner.run(new Configuration(),new Processer(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		String tmpPath = "/user/bilinhadoop/tmp/";
//		String tmpPath = "/home/luo/sample/tmp/";
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(new URI(args[1]),conf);
		FileSystem fsLocal = FileSystem.get(new URI(tmpPath),conf);
		fsLocal.delete(new Path(tmpPath), true);
		conf.set("CONFPATH", args[2]);
		
		String datetime = args[3];
		if(datetime.length() != 8){
			System.err.println("error argments with datetime");
			System.exit(1);
		}
		
		conf.set("datetime", datetime);
		
		Job job = Job.getInstance(conf, "Frequence");
		job.setJarByClass(Processer.class);
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		job.setCombinerClass(ProcessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
//		if(geoPath != null)
//			job.addCacheFile(URI.create(geoPath));
		ArrayList<String> inputs = getInputList(args[0],args[4],datetime);
		if(inputs != null)
			for(String path : inputs){
				FileInputFormat.addInputPath(job, new Path(path));
			}
		else
			System.exit(2);
		FileOutputFormat.setOutputPath(job, new Path(tmpPath));
		if(!job.waitForCompletion(true))
			return 1;
		Job job2 = Job.getInstance(conf,"sort");
		fs.delete(new Path(args[1]), true);
		FileInputFormat.addInputPath(job2, new Path(tmpPath));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setJarByClass(Processer.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		job2.setSortComparatorClass(SortWritable.class);
		job2.setNumReduceTasks(1);
		MultipleOutputs.setCountersEnabled(job2, true);
		job2.waitForCompletion(true);
		
		CounterGroup groupName = job2.getCounters().getGroup("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs");
		boolean isbidswitch = false;
		for(Counter name : groupName){
			String simpleName = name.getName();
			long lines = job2.getCounters().findCounter("LINE_NUM", simpleName.substring(0, simpleName.indexOf("."))).getValue();
			if(simpleName.contains(".frq") && !simpleName.contains("bidswitch")){
				BufferedWriter out = null;
				FSDataOutputStream fout = null;
				try{
					fout = fs.append(new Path(args[1].concat("/".concat(simpleName.replace(".frq", ".cnt")).concat("-r-00000"))));
//					fout = fs.create(new Path(args[1].concat("/".concat(simpleName.replace(".frq", ".cnt")).concat("-r-00000"))));
					out = new BufferedWriter(new OutputStreamWriter(fout,"utf-8"));
					out.write("KINDS\t".concat(""+name.getValue()));
					out.write("\nLINES\t".concat(""+lines));
					out.newLine();
					out.flush();
				}catch(FileNotFoundException e){
					
				}finally{
		        	if(out != null)
		        		out.close();
		        	if(fout != null)
		        		fout.close();
		        }
			}
			if(simpleName.contains("bidswitch") && isbidswitch == false)
				isbidswitch = true;
		}
		if(isbidswitch == true){
			long lines = job2.getCounters().findCounter("LINE_NUM", "bidswitch").getValue();
			BufferedWriter out = null;
			FSDataOutputStream fout = null;
			try{
				fout = fs.append(new Path(args[1].concat("/".concat("bidswitch.count.").concat(datetime)).concat(".cnt-r-00000")));
//				fout = fs.create(new Path(args[1].concat("/".concat("bidswitch.count.").concat(datetime)).concat(".cnt-r-00000")));
				out = new BufferedWriter(new OutputStreamWriter(fout,"utf-8"));
				out.write("bidswitch\tLINES\t".concat(""+lines));
				out.newLine();
				out.flush();
			}finally{
	        	if(out != null)
	        		out.close();
	        	if(fout != null)
	        		fout.close();
	        }
		}
		return 0;
	}

	private String getDate(String today,int dif) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date dt = format.parse(today);
		Calendar c = Calendar.getInstance();
		c.setTime(dt);
		c.add(Calendar.DATE, dif);
		//String yesterday = "."+c.get(Calendar.YEAR)+(c.get(Calendar.MONTH)+1)+c.get(Calendar.DAY_OF_MONTH);
		String yesterday=format.format(c.getTime());
		return yesterday;
	}
	
	private ArrayList<String> getInputList(String path,String country,String datetime) throws ParseException{
		ArrayList<String> inputList = new ArrayList<String>();
		String pre_path = path.concat("/*").concat(datetime);
		String yesterday = getDate(datetime,-1);
		String tomorrow = getDate(datetime,1);
		String pre_path_last = path.replaceAll("\\d{8}",yesterday).concat("/*").concat(yesterday);
		String pre_path_next = path.replaceAll("\\d{8}",tomorrow).concat("/*").concat(tomorrow);
		if(country.equalsIgnoreCase("cn")){
			inputList.add(pre_path.concat("0[0-9]*"));
			inputList.add(pre_path.concat("1[0-5]*"));
			inputList.add(pre_path_last.concat("1[6-9]*"));
			inputList.add(pre_path_last.concat("2[0-3]*"));
		}else if(country.equalsIgnoreCase("us")){
			inputList.add(pre_path_next.concat("0[0-4]*"));
			inputList.add(pre_path.concat("0[5-9]*"));
			inputList.add(pre_path.concat("1[0-9]*"));
			inputList.add(pre_path.concat("2[0-3]*"));
		}
		return inputList;
	}
}
