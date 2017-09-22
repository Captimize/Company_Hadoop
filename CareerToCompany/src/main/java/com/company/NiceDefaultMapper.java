package com.company;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NiceDefaultMapper extends
  Mapper<LongWritable, Text, CategoryCodeTaggedKey, Text> {

  // map 출력값
  //private final static IntWritable outputValue = new IntWritable(1);
  // map 출력키
  //private Text outputKey = new Text();
	CategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
  Text outValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    NiceBizInfoParser parser = new NiceBizInfoParser(value);

    // 출력키 설정
    if(parser.getsize()!="-"){
    	outputKey.setcategorycode(parser.getcode()+"&");
      outputKey.setTag(1);
      outValue.set(parser.getcompany()+"&"+parser.getowner()+"&"+parser.getidentificationnumber()+"&"+parser.getlocation()+"&"+parser.getsize()
      +"&"+parser.getstartyear()+"&"+parser.getcertificateyear());
    	//outputKey.set(parser.getcategory());
//parser.getsize()+"#"+parser.getcompany()+"#"+parser.getlocation()+"#"+
      context.write(outputKey, outValue);
   }
  }
}

