package mapred;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import util.Spliter;

public class SortWritable extends WritableComparator {

	public SortWritable(){
		super(Text.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ArrayList<String> line_a = Spliter.spliter(a.toString(),"\t");
		ArrayList<String> line_b = Spliter.spliter(b.toString(), "\t");
		if(line_a.size()!= 2 || line_b.size() != 2)
			return 0;
		else if(Long.parseLong(line_a.get(1)) > Long.parseLong(line_b.get(1)))
			return -1;
		else
			return 1;
		
	}
	
}
