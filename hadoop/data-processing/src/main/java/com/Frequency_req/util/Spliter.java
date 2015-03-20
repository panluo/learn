package util;

import java.util.ArrayList;

public class Spliter {
	public static ArrayList<String> spliter(String line,String separator){
		ArrayList<String> splited = new ArrayList<String>();
		int pos=0;
		int end=0;
		while((end = line.indexOf(separator,pos)) != -1){
			splited.add(line.substring(pos, end));
			pos = end + separator.length();
		}
		splited.add(line.substring(pos));
		return splited;
	}
}
