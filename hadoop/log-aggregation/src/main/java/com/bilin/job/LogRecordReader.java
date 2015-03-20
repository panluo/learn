package com.bilin.job;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class LogRecordReader extends RecordReader<NullWritable, Text>{
	  private static final Log LOG = LogFactory.getLog(LogRecordReader.class);
	  public static final String MAX_LINE_LENGTH = 
	    "mapreduce.input.linerecordreader.line.maxlength";

	  private long start;
	  private long pos;
	  private long end;
	  private LineReader in;
	  private FSDataInputStream fileIn;
	  private Seekable filePosition;
	  private int maxLineLength;
	  private NullWritable key = NullWritable.get();
	  private Text value;
	  private boolean isCompressedInput;
	  private Decompressor decompressor;
	  private byte[] recordDelimiterBytes;

	  public LogRecordReader() {
	  }

	  public LogRecordReader(byte[] recordDelimiter) {
	    this.recordDelimiterBytes = recordDelimiter;
	  }

	  public void initialize(InputSplit genericSplit,
	                         TaskAttemptContext context) throws IOException {
	    FileSplit split = (FileSplit) genericSplit;
	    Configuration job = context.getConfiguration();
	    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
	    start = split.getStart();
	    end = start + split.getLength();
	    final Path file = split.getPath();

	    // open the file and seek to the start of the split
	    final FileSystem fs = file.getFileSystem(job);
	    fileIn = fs.open(file);
	    
	    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
	    if (null!=codec) {
	      isCompressedInput = true;	
	      decompressor = CodecPool.getDecompressor(codec);
	      if (codec instanceof SplittableCompressionCodec) {
	        final SplitCompressionInputStream cIn =
	          ((SplittableCompressionCodec)codec).createInputStream(
	            fileIn, decompressor, start, end,
	            SplittableCompressionCodec.READ_MODE.BYBLOCK);
	        if (null == this.recordDelimiterBytes){
	          in = new LineReader(cIn, job);
	        } else {
	          in = new LineReader(cIn, job, this.recordDelimiterBytes);
	        }

	        start = cIn.getAdjustedStart();
	        end = cIn.getAdjustedEnd();
	        filePosition = cIn;
	      } else {
	        if (null == this.recordDelimiterBytes) {
	          in = new LineReader(codec.createInputStream(fileIn, decompressor),
	              job);
	        } else {
	          in = new LineReader(codec.createInputStream(fileIn,
	              decompressor), job, this.recordDelimiterBytes);
	        }
	        filePosition = fileIn;
	      }
	    } else {
	      fileIn.seek(start);
	      if (null == this.recordDelimiterBytes){
	        in = new LineReader(fileIn, job);
	      } else {
	        in = new LineReader(fileIn, job, this.recordDelimiterBytes);
	      }

	      filePosition = fileIn;
	    }
	    // If this is not the first split, we always throw away first record
	    // because we always (except the last split) read one extra line in
	    // next() method.
	    if (start != 0) {
	      start += in.readLine(new Text(), 0, maxBytesToConsume(start));
	    }
	    this.pos = start;
	  }
	  

	  private int maxBytesToConsume(long pos) {
	    return isCompressedInput
	      ? Integer.MAX_VALUE
	      : (int) Math.min(Integer.MAX_VALUE, end - pos);
	  }

	  private long getFilePosition() throws IOException {
	    long retVal;
	    if (isCompressedInput && null != filePosition) {
	      retVal = filePosition.getPos();
	    } else {
	      retVal = pos;
	    }
	    return retVal;
	  }

	  public boolean nextKeyValue() throws IOException {
	    if (value == null) {
	      value = new Text();
	    }
	    int newSize = 0;
	    // We always read one extra line, which lies outside the upper
	    // split limit i.e. (end - 1)
	    while (getFilePosition() <= end) {
	      newSize = in.readLine(value, maxLineLength,
	          Math.max(maxBytesToConsume(pos), maxLineLength));
	      pos += newSize;
	      if (newSize < maxLineLength) {
	        break;
	      }

	      // line too long. try again
	      LOG.info("Skipped line of size " + newSize + " at pos " + 
	               (pos - newSize));
	    }
	    if (newSize == 0) {
	      value = null;
	      return false;
	    } else {
	      return true;
	    }
	  }

	  @Override
	  public NullWritable getCurrentKey() {
	    return key;
	  }

	  @Override
	  public Text getCurrentValue() {
	    return value;
	  }

	  /**
	   * Get the progress within the split
	   */
	  public float getProgress() throws IOException {
	    if (start == end) {
	      return 0.0f;
	    } else {
	      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
	    }
	  }
	  
	  public synchronized void close() throws IOException {
	    try {
	      if (in != null) {
	        in.close();
	      }
	    } finally {
	      if (decompressor != null) {
	        CodecPool.returnDecompressor(decompressor);
	      }
	    }
	  }
}
