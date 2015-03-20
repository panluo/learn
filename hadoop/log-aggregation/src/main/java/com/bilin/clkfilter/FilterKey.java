package com.bilin.clkfilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FilterKey implements WritableComparable<FilterKey> {
	private Text ip;
	private Text domain;
	private LongWritable time;
	
	public FilterKey(){
		ip = new Text();
		domain = new Text();
		time = new LongWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ip.readFields(in);
		domain.readFields(in);
		time.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		ip.write(out);
		domain.write(out);
		time.write(out);
	}
	
	public void set(String ip,String domain,long time){
		this.ip.set(ip);
		this.domain.set(domain); 
		this.time.set(time);
	}
	
	public void set(FilterKey fk){
		this.ip.set(fk.getIp().toString());
		this.domain.set(fk.getDomain().toString());
		time.set(fk.getTime().get());
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof FilterKey){
			FilterKey filterKey = (FilterKey) obj;
			return ip.equals(filterKey.getIp()) && domain.equals(filterKey.getDomain()) &&
					time.equals(filterKey.getTime());
		} 
		return false;
	}
	
	public boolean clkEquals(FilterKey filterKey){
		return ip.equals(filterKey.getIp()) && domain.equals(filterKey.getDomain());
	}
	
	@Override
	public int compareTo(FilterKey o) {
		int isequal = ip.compareTo(o.getIp());
		if(isequal != 0){
			return isequal;
		} else if(( isequal = domain.compareTo(o.getDomain())) != 0 ){
			return isequal;
		}
		return time.compareTo(o.getTime());
	}
	
	@Override
	public String toString() {
		return ip.toString() + "\t" + domain.toString()
				+ "\t" + time.toString();
	}

	public Text getIp() {
		return ip;
	}

	public Text getDomain() {
		return domain;
	}

	public LongWritable getTime() {
		return time;
	}

	
	

}
