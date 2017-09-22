package com.company;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.reducetest.TaggedKey;

public class MajorToNiceReducer extends Reducer<CategoryCodeTaggedKey, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();
private String[] ad;
  public void reduce(CategoryCodeTaggedKey key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    Text[] record1 = new Text[200000];
    
    Text info = new Text(iterator.next());	// input[0]
    ad = info.toString().trim().split("&");
    int a = 0;
    for(String pp : ad){
    while (iterator.hasNext()) {
    	
    	Text record = new Text(iterator.next());
    	record1[a]=record;
    	a++;
    }
	      for(int b=0;b<a;b++){
	      outputKey.set(key.getcategorycode()+"&");
	      outputValue = new Text(pp.trim()+record1[b].toString());
	      context.write(outputKey, outputValue);
    	
	      }
    	
    	
	    
    }
    
  }
}



