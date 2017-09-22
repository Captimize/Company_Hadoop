package com.company;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class MajorToNiceMapper2 extends Mapper<LongWritable, Text, CategoryCodeTaggedKey, Text> {
	CategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    NiceBizInfoParser parser = new NiceBizInfoParser(value,0);

    outputKey.setcategorycode(parser.getcode());
    outputKey.setTag(1);
    outValue.set("&"+parser.getcompany()+"&"+parser.getowner()+"&"+parser.getidentificationnumber()+"&"+parser.getlocation()+"&"+parser.getsize()
    +"&"+parser.getstartyear()+"&"+parser.getcertificateyear());

    context.write(outputKey, outValue);
  }
}

