package com.bilin.job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ReqExtractUrlReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long set_id = Integer.parseInt(getFileName(key));
        int url_count = 0;
        for (@SuppressWarnings("unused") Text value : values) {
            url_count++;
        }
        // adding list type to result
        mos.write(new Text("u \t" + String.valueOf(set_id)), new Text(this.getUrl(key) + "\t" + getTimeStamp(key) +
                "\t" + url_count), "extractUrl/urllist_" + String.valueOf(set_id));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    public String getFileName(Text key) {
        String k = key.toString();
        return k.substring(0, k.indexOf(".", 0));
    }

    public String getTimeStamp(Text key) {
        String t = key.toString();
        int index = t.indexOf(".", 0);
        return t.substring(index + 1, t.indexOf(".", index + 1));
    }

    public String getUrl(Text key) {
        String k = key.toString();
        int index = k.indexOf(".", 0);
        return k.substring(k.indexOf(".", index + 1) + 1);
    }
}