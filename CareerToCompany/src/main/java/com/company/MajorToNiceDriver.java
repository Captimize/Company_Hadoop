package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.filecache.DistributedCache;

import com.mapsidejoin.TaggedGroupKeyComparator;

class MajorToNiceDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Tool 인터페이스 실행
    int res = ToolRunner.run(new Configuration(), new MajorToNiceDriver(), args);
    System.out.println("MR-Job Result:" + res);
  }
  private static final String INTER_MAJORCODE = "INTER_MAJORCODE";
  private static final String INTER_MAJOR = "INTER_MAJOR";
  private static final String INTER_COMPANY = "INTER_COMPANY";
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    // 입력출 데이터 경로 확인
    if (otherArgs.length != 3) {
      System.err.println("Usage: MajorToNiceDriver <major> <company> <combined_output>");
      System.exit(2);
    }
//  ----------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1-0 - major information refactorying
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job1_0 = new Job(getConf(), "Pre1_0MajorToNiceDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job1_0, new Path(INTER_MAJORCODE));

    // Job 클래스 설정
    job1_0.setJarByClass(MajorToNiceDriver.class);

    // Reducer 클래스 설정
    //job1_0.setReducerClass(MajorToCodeReducer.class);


    // 입출력 데이터 포맷 설정
    job1_0.setInputFormatClass(TextInputFormat.class);
    job1_0.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job1_0.setOutputKeyClass(Text.class);
    job1_0.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job1_0, new Path(otherArgs[0]),
      TextInputFormat.class, MajorToCodeMapper.class);
    
    job1_0.waitForCompletion(true);
//  ----------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1 - major information refactorying
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job1 = new Job(getConf(), "Pre1MajorToNiceDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job1, new Path(INTER_MAJOR));

    // Job 클래스 설정
    job1.setJarByClass(MajorToNiceDriver.class);

    // Reducer 클래스 설정
    job1.setReducerClass(MajorToNicePreReducer1.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job1, new Path(INTER_MAJORCODE),
      TextInputFormat.class, MajorMapper.class);
    
    job1.waitForCompletion(true);
//  ----------------------------------------------------------------------------------
//    
//		Pre Job Operating Part2 - company info refactorying
//
//  ----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job2 = new Job(getConf(), "Pre2MajorToNiceDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job2, new Path(INTER_COMPANY));

    // Job 클래스 설정
    job2.setJarByClass(MajorToNiceDriver.class);
    job2.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
    job2.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
    job2.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

    // Reducer 클래스 설정
    job2.setReducerClass(MajorToNicePreReducer2.class);

    job2.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
    job2.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),
      TextInputFormat.class, NiceMapper.class);

    job2.waitForCompletion(true);
//  ----------------------------------------------------------------------------------
//  
//		Main Job Operating Part - combine Major information and company information
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job3 = new Job(getConf(), "MajorToNiceDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[2]));

    // Job 클래스 설정
    job3.setJarByClass(MajorToNiceDriver.class);
    job3.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
    job3.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
    job3.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

    // Reducer 클래스 설정
    job3.setReducerClass(MajorToNiceReducer.class);

    job3.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
    job3.setMapOutputValueClass(Text.class);
    
    // 입출력 데이터 포맷 설정
    job3.setInputFormatClass(TextInputFormat.class);
    job3.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job3, new Path(INTER_MAJOR),
      TextInputFormat.class, MajorToNiceMapper1.class);
    MultipleInputs.addInputPath(job3, new Path(INTER_COMPANY),
      TextInputFormat.class, MajorToNiceMapper2.class);

    return job3.waitForCompletion(true) ? 0:1;
    
    
  }
}