package com.company;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class TaxArrangeDriver {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // 입출력 데이터 경로 확인
    if (args.length != 2) {
      System.err.println("Usage: CompanyDriver <input> <output>");
      System.exit(2);
    }
    // Job 이름 설정
    Job job = new Job(conf, "CompanyDriver");
 // Job 클래스 설정
    job.setJarByClass(TaxArrangeDriver.class);
    // 입출력 데이터 경로 설정
    FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

    
    // Mapper 클래스 설정
    job.setMapperClass(TaxMapper.class);
    
		// Reducer 클래스 설정
    job.setReducerClass(TaxReducer.class);
    
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // 입출력 데이터 포맷 설정

    job.setInputFormatClass(TextInputFormat.class);
		
    job.setOutputFormatClass(TextOutputFormat.class);
    // 출력키 및 출력값 유형 설정
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.waitForCompletion(true);
  }
}
