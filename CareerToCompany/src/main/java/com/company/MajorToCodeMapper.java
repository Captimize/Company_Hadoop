package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MajorToCodeMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  // map 출력값
  // map 출력키
  private Text outputKey = new Text();
  private final static Text outputValue = new Text();
  
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    MajorToCodeParser parser = new MajorToCodeParser(value);
    
       outputKey.set(parser.getmajor() + "#" + parser.getcode());
     //outputValue.set(parser.getcode());
    
        context.write(outputKey, outputValue);
    
   
    
    
  }
}