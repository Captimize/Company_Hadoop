/*package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CareerMapper extends
  Mapper<LongWritable, Text, Text, IntWritable> {

  // map 출력값
  // map 출력키
  private Text outputKey = new Text();
  private final static IntWritable outputValue = new IntWritable(1);
  
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    CareerInfoParser parser = new CareerInfoParser(value);

    // 출력키 설정
    
    	outputKey.set(parser.getmajor()+"#"+parser.getdetail());
//parser.getsize()+"#"+parser.getcompany()+"#"+parser.getlocation()+"#"+
      context.write(outputKey, outputValue);
    }
  
}*/
