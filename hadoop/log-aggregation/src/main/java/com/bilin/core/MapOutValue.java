package com.bilin.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MapOutValue implements Writable {

    private IntWritable count;
    private DoubleWritable cpm;
    private Text bilin_user_id;

    public MapOutValue() {
        this.setValues(0, 0.0, "");
    }

    public MapOutValue(int count) {
        this.count = new IntWritable(count);
        this.cpm = new DoubleWritable();
        this.bilin_user_id = new Text();
    }

    public MapOutValue(int count, double cpm) {
        this.count = new IntWritable(count);
        this.cpm = new DoubleWritable(cpm);
        this.bilin_user_id = new Text();
    }

    public MapOutValue(int count, double cpm, String bilin_user_id) {
        this.count = new IntWritable(count);
        this.cpm = new DoubleWritable(cpm);
        this.bilin_user_id = new Text(bilin_user_id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count.readFields(in);
        this.cpm.readFields(in);
        this.bilin_user_id.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.count.write(out);
        this.cpm.write(out);
        this.bilin_user_id.write(out);
    }

    public void setValues(int count, double cpm, String bilin_user_id) {
        this.count = new IntWritable(count);
        this.cpm = new DoubleWritable(cpm);
        this.bilin_user_id = new Text(bilin_user_id);
    }

    public void setValues(int count, double cpm) {
        this.count = new IntWritable(count);
        this.cpm = new DoubleWritable(cpm);
    }

    public int getCount() {
        return count.get();
    }

    public double getCpm() {
        return cpm.get();
    }
    
    public String getUserId(){
    	return bilin_user_id.toString();
    }
}