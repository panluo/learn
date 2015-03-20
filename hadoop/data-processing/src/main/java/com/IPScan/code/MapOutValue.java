package code;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class MapOutValue implements Writable{
	
	private LongWritable day_num;
	private LongWritable night_num;
	private BooleanWritable isweekend;
	
	

	@Override
	public void write(DataOutput out) throws IOException {
		this.day_num.write(out);
		this.night_num.write(out);
		this.isweekend.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.day_num.readFields(in);
		this.night_num.readFields(in);
		this.isweekend.readFields(in);
	}
	public MapOutValue(){
		this.day_num = new LongWritable(0);
		this.night_num = new LongWritable(0);
		this.isweekend = new BooleanWritable(false);
	}
	
	public MapOutValue(long day_num,long night_num,boolean isweekend){
		this.day_num = new LongWritable(day_num);
		this.night_num = new LongWritable(night_num);
		this.isweekend = new BooleanWritable(isweekend);
	}
	public long getDayNum(){
		return this.day_num.get();
	}
	
	public long getNightNum(){
		return this.night_num.get();
	}
	
	public boolean isWeekend(){
		return this.isweekend.get();
	}

	@Override
	public String toString() {
		return this.day_num+"	"+this.night_num+"	"+this.isweekend;
	}
	
}
