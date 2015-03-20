package com.bilin.ip;

import java.util.StringTokenizer;

public class IpInfo {
	private long from;
	private long to;
	private String code;
	
	public IpInfo(){
		
	}
	
	public IpInfo(long from, long to, String code ){
		this.from = from;
		this.to = to;
		this.code = code;
	}
	
	public void setValue(long from, long to, String code){
		this.from = from;
		this.to = to;
		this.code = code;
	}
	
	public long ipToLong(String ipString){
    	long result = 0;  
	    StringTokenizer token = new StringTokenizer(ipString,".");  
	    result += Long.parseLong(token.nextToken())<<24;  
	    result += Long.parseLong(token.nextToken())<<16;  
	    result += Long.parseLong(token.nextToken())<<8;  
	    result += Long.parseLong(token.nextToken());  
	    return result;
    }

	public long getFrom() {
		return from;
	}

	public void setFrom(long from) {
		this.from = from;
	}

	public long getTo() {
		return to;
	}

	public void setTo(long to) {
		this.to = to;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}
	
	
}
