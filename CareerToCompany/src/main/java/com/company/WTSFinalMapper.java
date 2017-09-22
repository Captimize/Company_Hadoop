package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WTSFinalMapper extends
  Mapper<LongWritable, Text, Text, IntWritable> {
	//ategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
	private final static IntWritable outValue = new IntWritable(1);
	Text outputKey = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  WTSFinalParser parser = new WTSFinalParser(value);
    outputKey.set(parser.getregion()+"&"+parser.getstatistics());
    context.write(outputKey, outValue);
    
    
  }
}
