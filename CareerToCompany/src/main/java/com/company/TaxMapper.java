package com.company;


//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaxMapper extends
  Mapper<LongWritable, Text, Text, Text> {
	//ategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
	Text outValue = new Text();
	Text outputKey = new Text();
  // map 출력값
  //private final static IntWritable outputValue = new IntWritable(1);
  // map 출력키
  //private Text outputKey = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    TaxInfoParser parser = new TaxInfoParser(value);
    outputKey.set(parser.getcategorycode());
    //outputKey.setTag(1);
    outValue.set(parser.getsize()+"#"+parser.getsex()+"#"+parser.getage()+"#"+parser.gettotalpay()+"#"+parser.getworkday());
    context.write(outputKey, outValue);
    
    //outputKey.set(parser.getcategory());
    //context.write(outputKey, outputValue);
    
  }
}
