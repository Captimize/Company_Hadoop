package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MajorMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  // map 출력값
  //private final static IntWritable outputValue = new IntWritable(1);
  // map 출력키
  //private Text outputKey = new Text();
	Text outputKey = new Text();
  Text outValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    MajorInfoParser parser = new MajorInfoParser(value);
    //outValue.set("-> major : "+parser.getmajor());
    //outValue.set(parser.gettest());
    //outputKey.setmajor(parser.getmajor());
    outValue.set(parser.getmajor()+"&");
    // 출력키 설정
    for(int a=0;a<parser.getcount();a++){
    	outputKey.set(parser.getcode(a)+"%");
    	context.write(outputKey, outValue);
    }
      //outValue.set(parser.getcompany()+"#"+parser.getowner()+"#"+parser.getlocation()+"#"+parser.getidentificationnumber()+"#"+parser.getsize()+"#"+
      //parser.getstartyear()+"#"+parser.getcertificateyear());
    	//outputKey.set(parser.getcategory());
//parser.getsize()+"#"+parser.getcompany()+"#"+parser.getlocation()+"#"+
      //context.write(outputKey, outValue);
  }
}

