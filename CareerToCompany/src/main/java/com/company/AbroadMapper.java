package com.company;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AbroadMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  // map 출력값
  // map 출력키
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    AbroadInfoParser parser = new AbroadInfoParser(value);

    // 출력키 설정
    if((parser.getcode()!=null)&&(parser.getcode()!="00")){
    	outputKey.set(parser.getcode()+"&" + parser.getcompanycountry() + "&" + parser.getcompanyname()
    	+	"&" + parser.getOwner() + "&"+parser.getAddress()  + "&" + parser.getSite()+ "&" + parser.getCompNum());
      context.write(outputKey, outputValue);
    }
  }
}
