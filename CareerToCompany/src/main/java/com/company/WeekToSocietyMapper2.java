package com.company;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class WeekToSocietyMapper2 extends Mapper<LongWritable, Text, CategoryCodeTaggedKey, Text> {
	CategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  SocietyCompanyInfoParser parser = new SocietyCompanyInfoParser(value,0,0);

    outputKey.setcategorycode(parser.getcompanylocation().trim());
    outputKey.setTag(1);
    outValue.set(parser.getdetail().trim());

    context.write(outputKey, outValue);
  }
}

