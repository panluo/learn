package com.bilin.core;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeekUserMapOutValue implements Writable {

    private Text site_category;
    private Text page_category;
    private Text max_time_stamp;
    private Text min_time_stamp;
    private Text total;

    public WeekUserMapOutValue() {
        this.setValues("", "", "", "", "");
    }

    public WeekUserMapOutValue(String site_category, String page_category, String max_time_stamp, String min_time_stamp, String total) {
        this.site_category = new Text(site_category);
        this.page_category = new Text(page_category);
        this.max_time_stamp = new Text(max_time_stamp);
        this.min_time_stamp = new Text(min_time_stamp);
        this.total = new Text(total);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.site_category.readFields(in);
        this.page_category.readFields(in);
        this.max_time_stamp.readFields(in);
        this.min_time_stamp.readFields(in);
        this.total.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.site_category.write(out);
        this.page_category.write(out);
        this.max_time_stamp.write(out);
        this.min_time_stamp.write(out);
        this.total.write(out);
    }

    public void setValues(String site_category, String page_category, String max_time_stamp, String min_time_stamp, String total) {
        this.site_category = new Text(site_category);
        this.page_category = new Text(page_category);
        this.max_time_stamp = new Text(max_time_stamp);
        this.min_time_stamp = new Text(min_time_stamp);
        this.total = new Text(total);
    }

    public String getSite_category() {
        return site_category.toString();
    }

    public String getPage_category() {
        return page_category.toString();
    }

    public String getMax_time_stamp() {
        return max_time_stamp.toString();
    }

    public String getTotal() {
        return total.toString();
    }

    public String getMin_time_stamp() {
        return min_time_stamp.toString();
    }
}