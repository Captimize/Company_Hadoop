package com.company;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class SocietyWeekPeopleMapper extends
  Mapper<LongWritable, Text, Text, Text> {

	private String[] ad;
	private String aa;
	Text outputKey = new Text();
  Text outValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  SocietyWeekPeopleInfoParser parser = new SocietyWeekPeopleInfoParser(value);
    if(parser.getweekday()!=null){
    	outValue.set(parser.getage()+"&"+parser.getsex()+"&"+parser.getcategory());
    	if(parser.getweekday().toString().contains("#")){
	    	ad = parser.getweekday().trim().split("#"); // one person's many weekday split
	    	for(int b=0;b<ad.length-1;b++){
	    			if(ad[b].contains("세종특별자치시")){
	    				ad[b]="세종특별자치시";
	    			}else if(ad[b].contains("부산광역시")){
	    				ad[b]="부산광역시";
	    			}else if(ad[b].contains("울산광역시")){
	    				ad[b]="울산광역시";
	    			}else if(ad[b].contains("인천광역시")){
	    				ad[b]="인천광역시";
	    			}else if(ad[b].equalsIgnoreCase("경기도 시")){
	    				ad[b]="경기도 시흥시";
	    			}
	    		outputKey.set(ad[b]+"%");
	    		context.write(outputKey, outValue);
	    		}
	    }else{
	    	aa=parser.getweekday().trim();
	    	if(aa.contains("세종특별자치시")){
				aa="세종특별자치시";
			}else if(aa.contains("부산광역시")){
				aa="부산광역시";
			}else if(aa.contains("울산광역시")){
				aa="울산광역시";
			}else if(aa.contains("인천광역시")){
				aa="인천광역시";
			}else if(aa.equalsIgnoreCase("경기도 시")){
				aa="경기도 시흥시";
			}
	    	outputKey.set(aa+"%");
	    	
	    	context.write(outputKey, outValue);
	    	}
    	//outputKey.set(parser.getweekday().toString().trim()+"%");
    	
    	//context.write(outputKey, outValue);
    	}
  }
}
/*
 * package com.company;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class SocietyWeekPeopleMapper2 extends
  Mapper<LongWritable, Text, Text, Text> {

  
	Text outputKey = new Text();
  Text outValue = new Text();
  private String[] ad;
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  SocietyWeekPeopleInfoParser parser = new SocietyWeekPeopleInfoParser(value,0);	//overloading
	  outValue.set(parser.getdetail());
	  //Text[] record1 = new Text[30];
	    if(parser.getweekday().toString().contains("#")){
	    	ad = parser.getweekday().toString().trim().split("#"); // one person's many weekday split
	    	for(int b=0;b<ad.length-1;b++){
	    			if(ad[b].contains("세종특별자치시")){
	    				ad[b]="세종특별자치시";
	    			}else if(ad[b].contains("부산광역시")){
	    				ad[b]="부산광역시";
	    			}else if(ad[b].contains("울산광역시")){
	    				ad[b]="울산광역시";
	    			}else if(ad[b].contains("인천광역시")){
	    				ad[b]="인천광역시";
	    			}else if(ad[b].equalsIgnoreCase("경기도 시")){
	    				ad[b]="경기도 시흥시";
	    			}
	    		outputKey.set(ad[b]);
	    		context.write(outputKey, outValue);
	    		}
	    }else{
	    	ad[0]=parser.getweekday().toString().trim();
	    	if(ad[0].contains("세종특별자치시")){
				ad[0]="세종특별자치시";
			}else if(ad[0].contains("부산광역시")){
				ad[0]="부산광역시";
			}else if(ad[0].contains("울산광역시")){
				ad[0]="울산광역시";
			}else if(ad[0].contains("인천광역시")){
				ad[0]="인천광역시";
			}else if(ad[0].equalsIgnoreCase("경기도 시")){
				ad[0]="경기도 시";
			}
	    	outputKey.set(ad[0]);
	    	context.write(outputKey, outValue);
	    	}
	    
	  //outputKey.set(parser.getweekday());
	  //outValue.set(parser.getcompanylocation());
    //outValue.set(parser.getdetail());
    // 출력키 설정
    
    //context.write(outputKey, outValue);
    
  }
}

*/
