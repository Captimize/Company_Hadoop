package com.reducetest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWithReducesideJoin extends
  Mapper<LongWritable, Text, TaggedKey, Text> {

  TaggedKey outputKey = new TaggedKey();
  Text outValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
    outputKey.setCarrierCode(parser.getUniqueCarrier());
    outputKey.setTag(1);
    outValue.set("2:->"+value);
    context.write(outputKey, outValue);
  }
}
