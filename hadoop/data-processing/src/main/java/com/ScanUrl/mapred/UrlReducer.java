package mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Reducer<Text, Text, Text, Text>.Context arg2) throws IOException,
			InterruptedException {
		Set<String> tmp = new HashSet<String>();
		
		int num = 0;
		for(Text key : arg1){
			tmp.add(key.toString());
			num++;
		}
		
		if(num >= 10){
			StringBuffer sb = new StringBuffer();
		
			for(String size : tmp){
				sb.append(size);
				sb.append(",");
			}
			arg2.write(arg0, new Text(sb.substring(0, sb.length() - 1)));
		}
	}
}
