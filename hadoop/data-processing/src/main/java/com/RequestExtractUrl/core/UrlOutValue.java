package com.bilin.core;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class UrlOutValue implements Writable{
    private Text url;
    private IntWritable count;

    public UrlOutValue(){
        this.setValue("", 0);
    }

    public UrlOutValue(String str, int count){
        this.setValue(str,count);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.url.write(dataOutput);
        this.count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.url.readFields(dataInput);
        this.count.readFields(dataInput);
    }

    public void setValue(String str, int count){
        this.url = new Text(str);
        this.count = new IntWritable(count);
    }

    public String getUrl() {
        return url.toString();
    }

    public int getCount() {
        return count.get();
    }
}
