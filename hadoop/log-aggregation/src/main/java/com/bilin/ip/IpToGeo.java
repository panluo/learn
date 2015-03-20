package com.bilin.ip;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

public class IpToGeo {
	
	private static IpInfo[] ipInfos;
	
	public static void loadGeoFile(String filePath,FileSystem fs) throws IOException{
		ipInfos = GeoInfoLoad.getInstance().loadGeoFile(filePath,fs);
	}
	
	public static String getGeo(String ip) throws IOException{
		return lookup(ip);
	}
	
	public static String getGeo(String ip ,String filePath,FileSystem fs) throws IOException{
		ipInfos = GeoInfoLoad.getInstance().loadGeoFile(filePath,fs);
		return lookup(ip);
	}
	
	public static String lookup(String ipString){
		String code = null;
		long ipLong = new IpInfo().ipToLong(ipString);
		int start = 0;
		int end = ipInfos.length -1;
		int mid;
		
		if(ipString == null || ipString.equals(" "))
			return null;
		
		while(start <= end){
			mid = (start + end) / 2;
			if(ipLong >= ipInfos[mid].getFrom() && ipLong <= ipInfos[mid].getTo()){
				return ipInfos[mid].getCode();
			}else if(ipLong > ipInfos[mid].getTo()){
				start = mid + 1;
			}else {
				end = mid - 1;
			}
		}
		
		return code;
	}
	
}
