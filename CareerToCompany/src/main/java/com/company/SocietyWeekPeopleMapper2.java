package com.company;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class SocietyWeekPeopleMapper2 extends
  Mapper<LongWritable, Text, Text, Text> {

  
	Text outputKey = new Text();
  Text outValue = new Text();
  private String[] ad;
  private String aa;
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  SocietyWeekPeopleInfoParser parser = new SocietyWeekPeopleInfoParser(value,0);	//overloading
	  outValue.set(parser.getdetail());
	  //Text[] record1 = new Text[30];
	  if(parser.getweekday2()!=null){
	    if(!parser.getweekday2().contains("#")){
	    	aa = parser.getweekday2().trim();
	    	outputKey.set(aa);
	    	context.write(outputKey, outValue);
	    }else{
	    	
	    	ad = parser.getweekday2().toString().trim().split("#"); // one person's many weekday split
	    	for(int b=0;b<ad.length-1;b++){
	    		outputKey.set(ad[b]);
	    		context.write(outputKey, outValue);
	    		}
	    	}
	  }
	  //outputKey.set(parser.getweekday());
	  //outValue.set(parser.getcompanylocation());
    //outValue.set(parser.getdetail());
    // 출력키 설정
    
    //context.write(outputKey, outValue);
    
  }
}

