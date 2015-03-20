package code;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import util.GetConf;
import util.IpInfo;
import util.LoadCZdb;

public class IpReduce extends Reducer<Text, MapOutValue, Text, Text> {
	private IpInfo[] IpInfos = null;
	private IpInfo ipinfo = new IpInfo();
	private Map<String,Float> proper = null;
	MultipleOutputs<Text, Text> multiple = null;
	MapOutValue weekend_value = null;
	MapOutValue val1=null,val2=null;
	
	@Override
	protected void setup(Reducer<Text, MapOutValue, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		URI[] localCacheFile = context.getCacheFiles();
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(localCacheFile[0],conf);
		IpInfos = LoadCZdb.getinstance().getIpInfo(new Path(localCacheFile[0]), fs);
		
		String path = conf.get("CONFPATH");
		GetConf.getinstance().LoadConf(path);
		proper = GetConf.getinstance().getProper();
		
		multiple = new MultipleOutputs<Text,Text>(context);
	}

	@Override
	protected void reduce(Text arg0, Iterable<MapOutValue> arg1,
			Reducer<Text, MapOutValue, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		int bus = 0,res = 0,uk = 0,days=0;
		long total = 0;
	
//		Random ra = new Random();
		for(MapOutValue value : arg1){
			
			total += value.getDayNum()+value.getNightNum();
			days++;
//			if(ra.nextInt()%1000 <= 1)
//				System.out.println(value.toString());
			
			if(!value.isWeekend()){
				long day_num = value.getDayNum();
				long night_num = value.getNightNum();
				
				if(day_num > proper.get("X") && night_num < proper.get("A")*day_num){
					bus++;
				}else if(day_num < proper.get("B")*night_num && night_num > proper.get("Y")){
					res++;
				}else
					uk++;
			}else{ 
				if(val1==null)
					val1=value;
				else if(val2==null)
					val2=value;
				else
					System.out.println("Error date with too many weekends");
			}

		}
		weekend_value = val1;
		for(int i=0;i<2;i++){
			if(weekend_value != null)
				if(weekend_value.getDayNum()+weekend_value.getNightNum() < proper.get("C")*total/days)
					bus++;
				else if(weekend_value.getDayNum()+weekend_value.getNightNum() > proper.get("D")*total/days)
					res++;
				else
					uk++;
			else
				break;
			weekend_value = val2;
		}
		val1=null;val2=null;
		
		String filename;
		if(bus+res+uk > proper.get("N")){
//		if(bus+res+uk > 0){.
			float T = proper.get("T");
			if((bus+res+uk)*T <= bus)
				filename = "business";
			else if((bus+res+uk)*T <= res)
				filename = "residential";
			else 
				filename = "unknown";
			
			multiple.write(arg0, new Text(lookup(arg0.toString())), filename);			
		}
	}
	
	@Override
	protected void cleanup(
			Reducer<Text, MapOutValue, Text, Text>.Context context)
			throws IOException, InterruptedException {
		multiple.close();
	}

	public String lookup(String ip){
		long ip_long = ipinfo.ipToLong(ip);
		int start = 0;
		int end = IpInfos.length-1;
		int mid;
		while(start <= end){
			mid = (start + end)/2;
			if(ip_long <= IpInfos[mid].getTo() && ip_long >= IpInfos[mid].getFrom())
				return IpInfos[mid].getCode();
			else if(ip_long > IpInfos[mid].getTo())
				start = mid + 1;
			else if(ip_long < IpInfos[mid].getFrom())
				end = mid -1;
		}
		return "";
	}
	
}
