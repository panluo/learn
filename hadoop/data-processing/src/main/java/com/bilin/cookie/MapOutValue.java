package cookie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MapOutValue implements Writable {

	private Text ip, geo, time, os, domain;
	public MapOutValue(Text ip, Text geo, Text time, Text os, Text domain){
		this.ip = ip;
		this.geo = geo;
		this.time = time;
		this.os = os;
		this.domain = domain;
	}
	
	public MapOutValue(){
		this.ip = new Text();
		this.geo = new Text();
		this.time = new Text();
		this.os = new Text();
		this.domain = new Text();
	}
	
	public MapOutValue(String ip, String geo, String time, String os, String domain){
		this.ip = new Text(ip);
		this.geo = new Text(geo);
		this.domain = new Text(domain);
		this.time = new Text(time);
		this.os = new Text(os);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.ip.write(out);
		this.geo.write(out);
		this.time.write(out);
		this.domain.write(out);
		this.os.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.ip.readFields(in);
		this.geo.readFields(in);
		this.time.readFields(in);
		this.domain.readFields(in);
		this.os.readFields(in);
	}

	@Override
	public String toString() {
		return getIp().concat("\t").concat(getGeo()).concat("\t").concat(getTime()).concat("\t").concat(getOS()).concat("\t").concat(getDomain());
	}

	public String getIp() {
		return this.ip.toString();
	}

	public String getGeo() {
		return this.geo.toString();
	}

	public String getTime() {
		return this.time.toString();
	}

	public String getDomain() {
		return this.domain.toString();
	}
	public String getOS(){
		return this.os.toString();
	}
	
	public void setGeo(String geo){
		this.geo = new Text(geo);
	}
}
