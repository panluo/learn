package cookie;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CookieReducer extends Reducer<Text,MapOutValue,Text,MapOutValue> {

	Map<String,MapOutValue> rep = null;
	Map<String,MapOutValue> cookie_filed = new HashMap<String,MapOutValue>();
	@Override
	protected void reduce(Text arg0, Iterable<MapOutValue> arg1,
			Reducer<Text, MapOutValue, Text, MapOutValue>.Context arg2)
			throws IOException, InterruptedException {
		rep = find_cookie(arg1);
		if(rep.size() > 1){
			MapOutValue tmp;
			for(Iterator<String> key = rep.keySet().iterator(); key.hasNext();){
				tmp = rep.get(key.next());
				if(tmp.getGeo().length() == 10)
					tmp.setGeo(GeoCodeToText.getInstance().getText(tmp.getGeo()));
				arg2.write(arg0, tmp);
			}
//			arg2.write(new Text(), new MapOutValue());
		}
	}
	
	public Map<String,MapOutValue> find_cookie(Iterable<MapOutValue> tmp){
		cookie_filed.clear();
		for(MapOutValue val : tmp){
			MapOutValue value = new MapOutValue(val.getIp(),val.getGeo(),val.getTime(),val.getOS(),val.getDomain());
			cookie_filed.put(val.getIp(), value);
		}
		return cookie_filed;
	}

	@Override
	protected void setup(
			Reducer<Text, MapOutValue, Text, MapOutValue>.Context context)
			throws IOException, InterruptedException {
		
		URI[] cachefiles = context.getCacheFiles();
		FileSystem fs = FileSystem.get(cachefiles[0],context.getConfiguration());
		GeoCodeToText.getInstance().loadfile(cachefiles[0].toString(), fs);
	}
}
