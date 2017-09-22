package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SocietyCompanyMapper2 extends
  Mapper<LongWritable, Text, Text, Text> {

  // map 출력값
  Text outputKey = new Text();
  Text outValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    SocietyCompanyInfoParser parser = new SocietyCompanyInfoParser(value,0);

    // 출력키 설정
    //if(parser.getsize()!="-"){
    	outputKey.set(parser.getcompanylocation2()+"%");
      outValue.set(parser.getcompanyname()+"&"+parser.getgoods());
    	//outputKey.set(parser.getcategory());
//parser.getsize()+"#"+parser.getcompany()+"#"+parser.getlocation()+"#"+
      context.write(outputKey, outValue);
   // }
  }
}

