package mapred;

import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public interface RunMapper {
	public Map<Text,LongWritable> run(Text lineOfLog,Map<String,Integer> logformat);
}