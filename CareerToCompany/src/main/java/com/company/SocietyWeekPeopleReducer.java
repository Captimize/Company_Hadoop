
package com.company;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.mapreduce.Reducer;

public class SocietyWeekPeopleReducer extends Reducer<Text, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();
  private String[] temp;
  private String categoryname;
  
  private float sex=0;
  private float age = 0;
  
  private int category_seecount=0;
  private int category_hearcount=0;
  private int category_bodycount=0;
  private String pattern1="##";
  DecimalFormat iformat = new DecimalFormat(pattern1);
  private String pattern2=".##";
  DecimalFormat dformat = new DecimalFormat(pattern2);
  private int number_of_worker=0;
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
	  	if(!key.toString().isEmpty()){
	  		Iterator<Text> iterator = values.iterator();
	  		outputKey.set(key+"%");
    
	  		Text info = new Text(iterator.next());	// input[0] 
	  		temp = info.toString().split("&");
	  		
    	//outputValue.set(outputValue.toString()+info.toString());
        while (iterator.hasNext()) {
          							// input[1]
          
        	age += Integer.parseInt(temp[0].trim());
        	
          if(!(temp[1].equalsIgnoreCase("1"))){
        	  sex += 1;
          }else{
          	sex += 2;
          			}
          
          if(!(temp[2].equalsIgnoreCase("1"))){
        	  category_seecount++;
          }else if(!(temp[2].equalsIgnoreCase("2"))){
        	  category_hearcount++;
          }else{
        	  category_bodycount++;
          			}
          
          info = iterator.next();
          temp = info.toString().split("&");
          number_of_worker++;
          //outputValue.set(outputValue.toString()+record.toString());
          
        		}
        
        sex = sex / number_of_worker -1;
        age = (age / number_of_worker) +2;
        
        outputValue.set(iformat.format(sex*100)+"&"+dformat.format(age)+"&"+category_seecount+"&"+category_hearcount+"&"+category_bodycount+"&"+number_of_worker);
        //outputValue.set(outputValue.toString());
        context.write(outputKey, outputValue);
        outputValue.set("");
        sex=0;
        age = 0;
        number_of_worker=0;
        category_seecount=0;
        category_hearcount=0;
        category_bodycount=0;
        
  	}
  }
  
  

}




