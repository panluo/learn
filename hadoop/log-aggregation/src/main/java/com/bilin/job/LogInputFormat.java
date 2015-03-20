package com.bilin.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.base.Charsets;

public class LogInputFormat extends FileInputFormat<NullWritable, Text> {

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		String delimiter = context.getConfiguration().get(
		        "textinputformat.record.delimiter");
		    byte[] recordDelimiterBytes = null;
		    if (null != delimiter)
		      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
		return new LogRecordReader(recordDelimiterBytes);
	}
	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    final CompressionCodec codec =
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    if (null == codec) {
	      return true;
	    }
	    return codec instanceof SplittableCompressionCodec;
	  }	
}
