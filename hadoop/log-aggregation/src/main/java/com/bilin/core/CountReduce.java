package com.bilin.core;

import java.util.Iterator;

public class CountReduce implements RunReduce {

	private static long result;
	private Iterator<MapOutValue> values;

	public String run(){
		result = 0;
		while(values.hasNext()){
			MapOutValue mvalue = (MapOutValue) values.next();
			result += mvalue.getCount();
		}
		return String.valueOf(result);
	}

	@Override
	public void setValue(Iterable<MapOutValue> mapOutValue) {
		Iterator<MapOutValue> ite = mapOutValue.iterator();
		values = ite;
	}
}
