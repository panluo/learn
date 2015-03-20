package code;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class RunMap {
	public static Map<String,MapOutValue> run(String lineOfIp,boolean isweekend,long limit){
		StringTokenizer line = new StringTokenizer(lineOfIp,"\t");
		Map<String,MapOutValue> result = new HashMap<String,MapOutValue>();
		if(line.countTokens() != 3)
			return null;
		else{
			String ip = line.nextToken();
			long day_num = Long.parseLong(line.nextToken());
			long night_num = Long.parseLong(line.nextToken());
			
			if(day_num + night_num < limit)
				return null;
			else{
				result.put(ip, new MapOutValue(day_num, night_num, isweekend));
				return result;
			}
		}
	}
}
