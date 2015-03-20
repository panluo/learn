package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PathGenerator {
	
	private SimpleDateFormat format;
	private Calendar cd;
	public PathGenerator(){
		this.format = new SimpleDateFormat("yyyyMMdd");
		this.cd = Calendar.getInstance();
	}
	
	public String getDateFromPath(String path){
		Pattern pattern = Pattern.compile("\\d{8}");
		Matcher m = pattern.matcher(path);
		if(m.find())
			return m.group();
		else
			return null;
	}
	
	public String nextday(String date) throws ParseException{
		Date dt = format.parse(date);
		cd.setTime(dt);
		cd.add(Calendar.DATE, 1);
		return format.format(cd.getTime());
	}
	
	public Boolean isWeekend(String path) throws ParseException{
		String date = getDateFromPath(path);
		if(date!=null){
			Date dt = format.parse(date);
			cd.setTime(dt);
			if(cd.get(Calendar.DAY_OF_WEEK)==1 || cd.get(Calendar.DAY_OF_WEEK)==7)
				return true;
			else
				return false;
		}else
			return null;
	}
	
	public String newPath(String path) throws ParseException{
		String nowday = getDateFromPath(path);
		String newday = nextday(nowday);
		if(newday==null){
			return null;
		}else{
			String newpath = path.replaceAll("\\d{8}", newday);
			return newpath;
		}
	}
}
