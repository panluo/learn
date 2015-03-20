package main;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import utils.DateTransformer;
import utils.Spliter;


public class test {

	public static void main(String[] args) throws UnsupportedEncodingException {
//		String time = DateTransformer.TimeFormatter("1416805199");
//		System.out.println(time);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		String[] aa= "us|||".split("\\|");
//		StringTokenizer bb = new StringTokenizer ("us||","|");
		ArrayList<String> cc = Spliter.splits("us|||", "|");
		System.out.println(cc.size());
		System.out.println(cc.get(0)+"aa");
		System.out.println(cc.get(1)+"bb");
		System.out.println(cc.get(2)+"cc");
		Calendar c = Calendar.getInstance();
		System.out.println(c.getTimeZone());
		c.setTimeZone(TimeZone.getTimeZone("UTC-5"));
		System.out.println(c.getTimeZone());
		
		
		String time = DateTransformer.UTCToEST("1417658401", "utc-5");
		Date dateTemp = new Date(1417658401);
		System.out.println(dateTemp);
		
		System.out.println("http://www.legacy.com/obituaries/fltimes/obituary.aspx?n=steven-e-webster&pid=173266529&fhid=10749".length());
		format.setTimeZone(TimeZone.getTimeZone("GMT-5"));
		System.out.println(format.format(dateTemp));
		
		System.out.println(time);
	}

}
