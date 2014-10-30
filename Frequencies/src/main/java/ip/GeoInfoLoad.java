package ip;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;

public class GeoInfoLoad {
	
	public static GeoInfoLoad geoInfo;
	public static final String SEPARATOR = ",";
	
	private IpInfo[] ipInfos;
	
	static {
        if (geoInfo == null) {
        	geoInfo = new GeoInfoLoad();
        }
    }

    public static GeoInfoLoad getInstance() {
        if (geoInfo == null) {
        	geoInfo = new GeoInfoLoad();
        }
        return geoInfo;
    }
    
    public IpInfo[] loadGeoFile(String filePath,FileSystem fs) throws IOException{
    	ipInfos = new IpInfo[countLine(new Path(filePath),fs)];
    	loadIpInfos(new Path(filePath),fs);
    	return ipInfos;
    }
    
    private void loadIpInfos(Path file,FileSystem fs) throws IOException{
    	IpInfo ipInfo = new IpInfo();
    	BufferedReader br = null;
    	int count = 0;
    	
    	try{
    		br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
    		String line = null;
    		while ((line = br.readLine())!= null) {
    			StringTokenizer token = new StringTokenizer(line,SEPARATOR);
    			long from = ipInfo.ipToLong(token.nextToken());
    			long to = ipInfo.ipToLong(token.nextToken());
    			String code = token.nextToken();
    			
    			IpInfo info = new IpInfo(from,to,code);
    			ipInfos[count ++] = info;
    		}
    	}finally{
    		if(br != null){
    			br.close();
    		}
    	}
    }
    
    public int countLine(Path file,FileSystem fs) throws IOException{
    	BufferedReader br = null;
    	int sum = 0;

    	try{
    		br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
    		while (br.readLine()!= null) {
    			sum ++;
    		}
    	}finally{
    		if(br != null){
    			br.close();
    		}
    	}
    	return sum;
    }
}
