package com.bilin.core;

//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserCateMapOutValue implements Writable {

    private Text site_category;
    private Text page_category;
    private Text time_stamp;

    public UserCateMapOutValue() {
        this.setValues("", "", "");
    }

    public UserCateMapOutValue(String site_category, String page_category, String time_stamp) {
        this.site_category = new Text(site_category);
        this.page_category = new Text(page_category);
        this.time_stamp = new Text(time_stamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.site_category.readFields(in);
        this.page_category.readFields(in);
        this.time_stamp.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.site_category.write(out);
        this.page_category.write(out);
        this.time_stamp.write(out);
    }

    public void setValues(String site_category, String page_category, String time_stamp) {
        this.site_category = new Text(site_category);
        this.page_category = new Text(page_category);
        this.time_stamp = new Text(time_stamp);
    }

    public String getSite_category() {
        return site_category.toString();
    }

    public String getPage_category() {
        return page_category.toString();
    }

    public String getTime_stamp() {
        return time_stamp.toString();
    }
}