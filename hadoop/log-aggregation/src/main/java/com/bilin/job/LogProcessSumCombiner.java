package com.bilin.job;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bilin.core.MapOutValue;

public class LogProcessSumCombiner extends Reducer<Text, MapOutValue, Text, MapOutValue> {
	private MapOutValue mov = new MapOutValue();
	private int count;
	private double cpm;
	
	@Override
	protected void reduce(Text key, Iterable<MapOutValue> mapOutValue,
			Context context)
			throws IOException, InterruptedException {
		Iterator<MapOutValue> ite = mapOutValue.iterator();
		
		count = 0;
		cpm = 0.0;
		while(ite.hasNext()){
			MapOutValue mov = ite.next();
			count += mov.getCount();
			cpm += mov.getCpm();
		}
		mov.setValues(count, cpm);
		context.write(key, mov);
	}

}
