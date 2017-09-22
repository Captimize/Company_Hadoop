package com.company;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SocietyCompanyReducer2 extends Reducer<Text, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();

  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    
    
    Text info = new Text(iterator.next());	// input[0] 
   
    while (iterator.hasNext()) {
      Text record = iterator.next();							// input[1]
      outputKey.set(key);
      outputValue = new Text(record.toString());
      context.write(outputKey, outputValue);
    }
  }
}