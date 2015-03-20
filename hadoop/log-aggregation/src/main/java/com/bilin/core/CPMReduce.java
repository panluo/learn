package com.bilin.core;

import java.util.Iterator;

public class CPMReduce implements RunReduce {
	
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
	
	public String run(){
		this.init();
		
		while(values.hasNext()){
			MapOutValue mvalue = (MapOutValue) values.next();
			count += mvalue.getCount();
			cpm += mvalue.getCpm();
		}
		return String.valueOf(count) + SEPERATOR + String.valueOf(cpm);
	}

}
