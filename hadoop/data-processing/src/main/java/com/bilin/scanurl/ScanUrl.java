package scanurl;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;


public class ScanUrl extends Configured implements Tool {
	
	/*
	 *  Get date after some day
	 */
	public static String getDate(int date, int after){
		SimpleDateFormat sdf=new SimpleDateFormat("YYYYMMdd");
		Calendar cl = Calendar.getInstance();
		int year = date/10000;
		int month = (date % 10000) / 100;
		int day = date % 100;
		cl.set(year, month - 1, day);
		cl.set(Calendar.DATE, day + after);
		return sdf.format(cl.getTime());
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Frequency");
		job.setJarByClass(ScanUrl.class);
		
		job.setMapperClass(ScanMapper.class);
		job.setCombinerClass(ScanReducer.class);
		job.setReducerClass(ScanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputDirRecursive(job, true);
		
		// add input path
		int startDay = Integer.parseInt(args[0]);
		String inputPath = "s3://bilin-data/hadoop/data_log/req/";
		for(int i = 0; i < Integer.parseInt(args[1]); i++){
			String tday = getDate(startDay,i);
			System.out.println("input: " + inputPath + tday + "/");
			FileInputFormat.addInputPath(job, new Path(inputPath + tday + "/"));
		}
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
//		job.setNumReduceTasks(1);
		
		job.addCacheFile(URI.create(args[3]));
		//Important
		MultipleOutputs.setCountersEnabled(job, true);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 4){
			System.out.println("Usage Frequency <in> <out>");
			System.exit(2);
		}
		ToolRunner.run(new ScanUrl(), args);
	}
	
	public static class ScanMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private List<String> urlList = new LinkedList<String>();
		private Text outKey = new Text();
		private IntWritable one = new IntWritable(1);
		private SimpleDateFormat sdf=new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
		
		private void loadIpInfos(Path file,FileSystem fs) throws IOException{
	    	BufferedReader br = null;
	    	
	    	try{
	    		br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
	    		String line = null;
	    		
	    		while ((line = br.readLine())!= null) {
	    			
	    			StringTokenizer token = new StringTokenizer(line);
	    			urlList.add(token.nextToken());
	    		}
	    	}finally{
	    		if(br != null){
	    			br.close();
	    		}
	    	}
	    }
		
		public ArrayList<String> split(String str, String delim) {
			ArrayList<String> splits = new ArrayList<String>();
	        int posBeg = 0, length = str.length(), posEnd = str.indexOf(delim, posBeg);
	        while (length != posEnd && -1 != posEnd) {
	            splits.add(str.substring(posBeg, posEnd));
	            posBeg = posEnd + delim.length();
	            posEnd = str.indexOf(delim, posBeg);
	        }
	        splits.add(str.substring(posBeg, length));
	        return splits;
	    }
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			URI[] localCacheFile = context.getCacheFiles();
			FileSystem fs = FileSystem.get(localCacheFile[0], new Configuration());
			
			loadIpInfos(new Path(localCacheFile[0].toString()),fs);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			ArrayList<String> splits = split(value.toString(),"\t");
			
			if(splits.size() != 64)
				return;
			
			
			boolean is = false;
			for(String url: urlList){
				if(url.equals(splits.get(11))){
					is = true;
					break;
				}
			}
			
			if(is == false){
				return;
			}else{
				
				String geoInfo = new String();
				
				if(splits.get(28) != null && splits.get(28) != ""){
					ArrayList<String> geos = split(splits.get(28),"|");
					geoInfo = geos.get(0) + "\t" + geos.get(1) + "\t" + geos.get(2);
//					int i;
//					for(i = 0; i < geos.length && i < 3; i ++){
//						geoInfo += geos[i] + "\t";
//					}
				}else{
					geoInfo = "\t" + "\t";
				}
				
				// time, ip, country, state, city, url
				Date date = new Date();
				date.setTime(Long.parseLong(splits.get(1) + "000"));
				
				String result = sdf.format(date) + "\t" + splits.get(6) + "\t" + geoInfo + "\t" + splits.get(11);
				outKey.set(result);
				context.write(outKey, one);
			}
			
		}
	}
	
	
	
	static class ScanReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private MultipleOutputs<Text, IntWritable> multipleOutputs;
		private IntWritable result = new IntWritable();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
			
		}
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			
			for(IntWritable val : value){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}	
	}
	
}
