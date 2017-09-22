package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FundingMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  // map 출력값
  // map 출력키
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    FundingInfoParser parser = new FundingInfoParser(value);

    // 출력키 설정
    outputKey.set(parser.getcode()+"&");
    outputValue.set(parser.getfundingcompany()+"&"+parser.getcategory()+"&"+parser.getdetail()+"&"+parser.getperiod()+"&"+parser.getfunding_cost());
     context.write(outputKey, outputValue);
    }
  
}
