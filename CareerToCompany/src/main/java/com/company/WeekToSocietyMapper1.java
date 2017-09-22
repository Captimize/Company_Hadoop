package com.company;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class WeekToSocietyMapper1 extends Mapper<LongWritable, Text, CategoryCodeTaggedKey, Text> {
	CategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  SocietyWeekPeopleInfoParser parser = new SocietyWeekPeopleInfoParser(value,0);

    outputKey.setcategorycode(parser.getweekday2());
    //outputKey.setmajor(parser.getmajor());
    outputKey.setTag(0);
    outValue.set(parser.getdetail());

    context.write(outputKey, outValue);
  }
}

