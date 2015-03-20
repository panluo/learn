package utils;

import java.util.ArrayList;

public class Spliter {
	public static ArrayList<String> splited(String line, String termination){
		ArrayList<String> LineOfLog = new ArrayList<String>();
		int start = 0,end = 0;
		while((end = line.indexOf(termination,start)) != -1){
			LineOfLog.add(line.substring(start, end));
			start = end + termination.length();
		}
		LineOfLog.add(line.substring(start));
		return LineOfLog;
	}
}
