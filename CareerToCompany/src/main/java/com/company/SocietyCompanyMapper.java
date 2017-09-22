package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class SocietyCompanyMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  
	Text outputKey = new Text();
  Text outValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  SocietyCompanyInfoParser parser = new SocietyCompanyInfoParser(value);
    
	  outputKey.set(parser.getcompanyname()+"%");
	  //outValue.set(parser.getcompanylocation());
    outValue.set(parser.getcompanylocation()+"&"+parser.getgoods());
    // 출력키 설정
    
    context.write(outputKey, outValue);
    
  }
}

