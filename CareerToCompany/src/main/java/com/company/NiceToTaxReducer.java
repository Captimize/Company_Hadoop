/*package com.company;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NiceToTaxReducer extends Reducer<CategoryCodeTaggedKey, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();

  public void reduce(CategoryCodeTaggedKey key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    // 항공사 이름 조회
    Text carrierName = new Text(iterator.next());	// input[0] 
    // 운항 지연 레코드 조회
    while (iterator.hasNext()) {
      Text record = iterator.next();							// input[1]
      outputKey.set(key.getcategorycode());
      outputValue = new Text(carrierName.toString() + "\t" + record.toString());
      context.write(outputKey, outputValue);
    }
  }
}*/