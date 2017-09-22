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

public class SocietyCompanyReducer extends Reducer<Text, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();

  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    outputKey.set(key);
    
    Text info = new Text(iterator.next());	// input[0] 
    
    	outputValue.set(info.toString());
        while (iterator.hasNext()) {
          Text record = iterator.next();							// input[1]
          
          
          
        		}
        context.write(outputKey, outputValue);
        outputValue.set("");
  }
}




