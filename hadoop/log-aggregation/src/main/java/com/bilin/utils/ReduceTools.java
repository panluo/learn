package com.bilin.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class ReduceTools {
	private Calendar c = Calendar.getInstance();
	public String getYest(){
		c.add(Calendar.DATE, -1);
		Date d = c.getTime();
		//String yesterday = "."+c.get(Calendar.YEAR)+(c.get(Calendar.MONTH)+1)+c.get(Calendar.DAY_OF_MONTH);
		SimpleDateFormat sp = new SimpleDateFormat("yyyyMMdd");
		String yesterday=sp.format(d);
		return yesterday;
	}
	
	public String getLastHour(){
		c.add(Calendar.HOUR_OF_DAY, -1);
		Date d = c.getTime();
		SimpleDateFormat sp = new SimpleDateFormat("yyyyMMddHH");
		return sp.format(d);
	}
}
