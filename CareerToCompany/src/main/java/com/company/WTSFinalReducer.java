
package com.company;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.mapreduce.Reducer;

public class WTSFinalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private IntWritable result = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {

	  int sum = 0;
	    for (IntWritable val : values) {
	      sum += val.get();
	    }
	    result.set(sum);
	    outputKey.set(key+"&");
	    context.write(outputKey, result);
  }
  

}




