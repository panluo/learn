package mapred;

import java.io.IOException;
import java.util.ArrayList;

import main.Config;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import util.Spliter;

public class SortReducer extends Reducer<Text, NullWritable, Text, LongWritable> {

	MultipleOutputs<Text, LongWritable> writer = null;
	String datetime = null;
	String exchange_name = null;
	@Override
	protected void setup(
			Reducer<Text, NullWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		datetime = context.getConfiguration().get("datetime");
		writer = new MultipleOutputs<Text, LongWritable>(context);
		try {
			Config.getInstance().loadConfig(context.getConfiguration().get("CONFPATH"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(Config.getInstance().getExchanges().contains("bidswitch")){
			exchange_name = "bidswitch";
		}
	}

	@Override
	protected void reduce(Text arg0, Iterable<NullWritable> arg1,
			Reducer<Text, NullWritable, Text, LongWritable>.Context arg2)
			throws IOException, InterruptedException {
		long nums = Long.parseLong(arg0.toString().substring(arg0.toString().indexOf("\t") + 1));
		if(nums <= 0){
			arg2.getCounter("Error_In_Sort", "Error_with_splite_nums").increment(1);
			return;
		}
		String keys = arg0.toString().substring(0, arg0.toString().indexOf("\t"));
		ArrayList<String> splited = Spliter.spliter(keys, "+=&=+");
		if(splited.size() != 3){
			String line = splited.get(0);
			
			if(line.startsWith("LINE_NUM_")){
				arg2.getCounter("LINE_NUM", line.substring(9)).increment(nums);
//				writer.write(new Text(line.substring(9) + "\tLINES"), new LongWritable(nums), line.substring(9) + ".lines.".concat(datetime).concat(".cnt"));
			}else if(line.startsWith("ssp_num_count")){
				String ssp = line.substring(line.lastIndexOf("_")+1);
				writer.write(new Text(ssp + "\tLINES"), new LongWritable(nums), "bidswitch.count.".concat(datetime).concat(".cnt"));
			}else if(line.startsWith("total_flow_")){
				writer.write(new Text(splited.get(1)), new LongWritable(nums), "total.".concat(line.substring(line.lastIndexOf("_")+1)).concat(".").concat(datetime).concat(".frq"));
			}else
				arg2.getCounter("Error_In_Sort", "Error_with_sorted_line").increment(1);
			return;
		}
		
		String key,filename;
		if(exchange_name == null || !exchange_name.equals("bidswitch")){
			exchange_name = splited.get(0);
			key = splited.get(2);
		}else
			key = splited.get(0).concat("\t").concat(splited.get(2));
		
		if(!key.equals("count"))
			filename = exchange_name.concat(".").concat(splited.get(1)).concat(".").concat(datetime).concat(".frq");
		else{
			filename = exchange_name.concat(".").concat(splited.get(1)).concat(".").concat(datetime).concat(".cnt");
			key = "TOTALS";
		}
		writer.write(new Text(key), new LongWritable(nums), filename);
			
	}

	@Override
	protected void cleanup(
			Reducer<Text, NullWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		writer.close();
	}
	
}
