package cookie;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GeoCodeToText {
	private static GeoCodeToText gc;
	private Map<Long,String> codeToText = new HashMap<Long,String>();
	
	static {
		if(gc == null){
			gc = new GeoCodeToText();
		}
	}
	
	public static GeoCodeToText getInstance(){
		if(gc == null)
			return new GeoCodeToText();
		return gc;
	}
	
	public void loadfile(String path, FileSystem fs) {
		BufferedReader br = null;
		try{
			FSDataInputStream in = fs.open(new Path(path));
			br = new BufferedReader(new InputStreamReader(in,Charsets.UTF_8));
			String line;
			while((line = br.readLine()) != null){
				StringTokenizer st = new StringTokenizer(line,"\t");
				if(st.countTokens() == 2){
					long geoCode = Long.parseLong(st.nextToken());
					codeToText.put(geoCode, st.nextToken());
				}
			}
		}catch(IOException e){
			System.err.println("I/O Error; check the geo file Exsits or not");
		}
	}
	
	public String getText(String geoCode){
		if(geoCode.equals("") || geoCode == null)
			return "unknown";
		String text = codeToText.get(Long.parseLong(geoCode));
		if(text == null){
			return "unknown";
		}
		return text;
	}
}
