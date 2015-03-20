package cookie;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CookieCombine extends CookieReducer {
	@Override
	protected void reduce(Text arg0, Iterable<MapOutValue> arg1,
			Reducer<Text, MapOutValue, Text, MapOutValue>.Context arg2)
			throws IOException, InterruptedException {
		rep = find_cookie(arg1);
		for(Iterator<String> key = rep.keySet().iterator(); key.hasNext();){
			arg2.write(arg0, rep.get(key.next()));
		}
	}	
}
