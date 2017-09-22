package com.company;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class MajorToNiceMapper1 extends Mapper<LongWritable, Text, CategoryCodeTaggedKey, Text> {
	CategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    MajorInfoParser parser = new MajorInfoParser(value,0);

    outputKey.setcategorycode(parser.gettest());
    //outputKey.setmajor(parser.getmajor());
    outputKey.setTag(0);
    outValue.set(parser.getmajor());

    context.write(outputKey, outValue);
  }
}

