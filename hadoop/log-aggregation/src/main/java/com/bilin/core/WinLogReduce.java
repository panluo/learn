package com.bilin.core;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class WinLogReduce implements RunReduce {
	
	private static long count;
	private static double cpm;
	private Iterator<MapOutValue> values;

	private static final String SEPERATOR = "\t";
	
	@Override
	public void setValue(Iterable<MapOutValue> mapOutValue) {
		Iterator<MapOutValue> ite = mapOutValue.iterator();
		values = ite;
	}
	
	public void init(){
		count = 0;
		cpm = 0.0;
	}

	@Override
	public String run() {
		this.init();
		
		Set<String> userSet = new HashSet<String>();
		while(values.hasNext()){
			MapOutValue mvalue = (MapOutValue) values.next();
			count += mvalue.getCount();
			cpm += mvalue.getCpm();
			userSet.add(mvalue.getUserId());
		}
		
		String result = String.valueOf(count) + SEPERATOR + String.valueOf(cpm) 
				+ SEPERATOR + String.valueOf(userSet.size());
		return result;
	}
	
}
