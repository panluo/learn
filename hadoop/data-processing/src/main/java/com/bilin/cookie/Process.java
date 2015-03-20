package cookie;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Process extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(new URI(args[1]),conf);
		fs.delete(new Path(args[1]),true);
		
		conf.set("CONF_PATH", args[2]);
		Job job = Job.getInstance(conf,"cookie scan");
		job.setJarByClass(Process.class);
		job.setMapperClass(CookieMapper.class);
		job.setReducerClass(CookieReducer.class);
		job.setCombinerClass(CookieCombine.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapOutValue.class);
		
		job.addCacheFile(new URI(args[3]));
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length < 3){
			System.out.println("usage <input> <output> <confpath>");
			return;
		}
		ToolRunner.run(new Configuration(),new Process(), args);
	}
}
