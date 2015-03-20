package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class LoadCZdb {
	private static LoadCZdb cz = null;
	private static IpInfo[] IpInfos = null;
	
	static {
		if(cz == null)
			cz = new LoadCZdb();
	}
	
	public static LoadCZdb getinstance(){
		if(cz == null)
			cz = new LoadCZdb();
		return cz;
	}
	
	public IpInfo[] loadfile(Path file,FileSystem fs) throws IOException{
		IpInfo ipinfo = new IpInfo();
		BufferedReader br = null;
		String line = null;
		int count = 0;
		
		try{
			br = new BufferedReader(new InputStreamReader(fs.open(file),Charsets.UTF_8));
			while((line = br.readLine()) != null){
				StringTokenizer lineOfCZ = new StringTokenizer(line,"\t");
				long from = ipinfo.ipToLong(lineOfCZ.nextToken().trim());
				long to = ipinfo.ipToLong(lineOfCZ.nextToken().trim());
				String code = lineOfCZ.nextToken().trim();
				IpInfos[count++] = new IpInfo(from,to,code);
			}
		}finally{
			if(br != null)
				br.close();
		}
		return IpInfos;
	}
	
	public int countLine(Path path,FileSystem fs) throws IOException{
    	BufferedReader br = null;
    	int sum = 0;

    	try{
    		br = new BufferedReader(new InputStreamReader(fs.open(path),Charsets.UTF_8));
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
	
	public IpInfo[] getIpInfo(Path path,FileSystem fs) throws IOException {
		IpInfos = new IpInfo[countLine(path,fs)];
		return loadfile(path,fs);
	}
}
