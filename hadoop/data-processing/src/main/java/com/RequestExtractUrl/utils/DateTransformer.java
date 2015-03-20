package com.bilin.utils;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateTransformer {
	
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm";
	
	public static String dateTransformBetweenTimeZone(Date sourceDate, DateFormat formatter,
			TimeZone sourceTimeZone, TimeZone targetTimeZone){
		Long targetTime = sourceDate.getTime() - sourceTimeZone.getRawOffset() + targetTimeZone.getRawOffset();
		return DateTransformer.getTime(new Date(targetTime), formatter);
	}
	public static String getTime(Date date,DateFormat formatter) {
		return formatter.format(date);
	}

	public static String ToEST(String time, String time_zone){
		TimeZone destTimeZone = TimeZone.getTimeZone("EST");
		Date sourceDate = new Date(Long.valueOf(time+"000"));
		
		if(time_zone.isEmpty())
			time_zone="8";
		TimeZone srcTimeZone = TimeZone.getTimeZone("GMT+"+time_zone);
		
		return DateTransformer.dateTransformBetweenTimeZone(sourceDate, new SimpleDateFormat(DATE_FORMAT), srcTimeZone, destTimeZone);
	}
	
	public static String UTCToEST(String time, String time_zone) throws UnsupportedEncodingException{
		SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT+":ss");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		if(time_zone.startsWith("UTC") || time_zone.startsWith("utc")){
//			String zone = time_zone.substring(3);
//			int subTime;
//			if(zone.startsWith("-")){
//				subTime = 0 - Integer.parseInt(zone.substring(1));
//			}else if(zone.startsWith("+"))
//				subTime = Integer.parseInt(zone.substring(1));
//			else
//				subTime = 0;
			
			Date sourceDate = new Date((Long.valueOf(time) - 5*3600)*1000);
			return getTime(sourceDate, format);
		}else{
			Date sourceDate_utc = new Date(Long.valueOf(time)*1000);
			return getTime(sourceDate_utc, format);
		}
	}
	
	public static String TimeFormatter(String time){
		Date sourceDate = new Date(Long.valueOf(time+"000"));
		return getTime(sourceDate,new SimpleDateFormat(DATE_FORMAT+":ss"));
	}
	
	public static String getDate(){
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DATE,0);
		Date d = c.getTime();
		//String yesterday = "."+c.get(Calendar.YEAR)+(c.get(Calendar.MONTH)+1)+c.get(Calendar.DAY_OF_MONTH);
		SimpleDateFormat sp = new SimpleDateFormat("yyyyMMdd");
		String today=sp.format(d);
		return today;
	}
}
