package com.bilin.core;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PixelMapOutValue implements Writable {

    private IntWritable count;
//    private Text user_id;

    public PixelMapOutValue() {
        this.setValues(0);
    }

    public PixelMapOutValue(int count) {
        this.count = new IntWritable(count);
//        this.user_id = new Text();
//        this.user_id = new Text();
    }

    public PixelMapOutValue(int count, String user_id) {
        this.count = new IntWritable(count);
//        this.user_id = new Text(user_id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count.readFields(in);
//        this.user_id.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.count.write(out);
//        this.user_id.write(out);
    }

    public void setValues(int count) {
        this.count = new IntWritable(count);
    }

    public int getCount() {
        return count.get();
    }

//    public String getUserId(){
//    	return user_id.toString();
//    }
}