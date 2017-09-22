package com.company;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class WeekToSocietyReducer extends Reducer<CategoryCodeTaggedKey, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();
private String ad;
  public void reduce(CategoryCodeTaggedKey key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

	  Iterator<Text> iterator = values.iterator();
	    // 항공사 이름 조회
	    Text week = new Text(iterator.next());
	    // 운항 지연 레코드 조회
	    while (iterator.hasNext()) {
	      Text company = iterator.next();
	      outputKey.set(key.getcategorycode()+"%");
	      outputValue = new Text(company.toString()+ "%" + week.toString() );
	      context.write(outputKey, outputValue);
	    }
  	}
}


