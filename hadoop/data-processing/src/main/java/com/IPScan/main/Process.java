package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Process {

	public static void main(String[] args) throws Exception{
		if(args.length < 3){
			System.out.println("main.IpScan <input> <output> <confpath>");
			System.exit(0);
		}else{
			ToolRunner.run(new Configuration(), new IpJobConfig(), args);
		}
	}
}
