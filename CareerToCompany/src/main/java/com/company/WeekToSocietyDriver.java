package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.filecache.DistributedCache;

import com.mapsidejoin.TaggedGroupKeyComparator;

class WeekToSocietyDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Tool 인터페이스 실행
    int res = ToolRunner.run(new Configuration(), new WeekToSocietyDriver(), args);
    System.out.println("MR-Job Result:" + res);
  }
  private static final String INTER_WEEKPEOPLE1 = "INTER_WEEKPEOPLE1";
  private static final String INTER_WEEKPEOPLE2 = "INTER_WEEKPEOPLE2";
  private static final String INTER_SOCIETYCOMPANY1 = "INTER_SOCIETYCOMPANY1";
  private static final String INTER_SOCIETYCOMPANY1_1 = "INTER_SOCIETYCOMPANY1_1";
  private static final String INTER_SOCIETYCOMPANY2 = "INTER_SOCIETYCOMPANY2";
  private static final String INTER_WTS = "INTER_WTS";
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    // 입력출 데이터 경로 확인
    if (otherArgs.length != 4) {
        //if (otherArgs.length != 1) {
      System.err.println("Usage: WeekToSocietyDriver <week> <societycompany> <combined_output>");
      System.exit(2);
    }
//  ----------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1 - major information refactorying
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job1 = new Job(getConf(), "WeekPeopleDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job1, new Path(INTER_WEEKPEOPLE1));

    // Job 클래스 설정
    job1.setJarByClass(WeekToSocietyDriver.class);

    // Reducer 클래스 설정
    //job1.setReducerClass(SocietyWeekPeopleReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job1, new Path(otherArgs[0]),
      TextInputFormat.class, SocietyWeekPeopleMapper.class);
    
    job1.waitForCompletion(true);
    
    
//  ----------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1 - major information refactorying
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job1_2 = new Job(getConf(), "WeekPeopleDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job1_2, new Path(INTER_WEEKPEOPLE2));

    // Job 클래스 설정
    job1_2.setJarByClass(WeekToSocietyDriver.class);

    // Reducer 클래스 설정
    job1_2.setReducerClass(SocietyWeekPeopleReducer.class);

    job1_2.setMapOutputKeyClass(Text.class);
    job1_2.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job1_2.setInputFormatClass(TextInputFormat.class);
    job1_2.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job1_2.setOutputKeyClass(Text.class);
    job1_2.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job1_2, new Path(INTER_WEEKPEOPLE1),
      TextInputFormat.class, SocietyWeekPeopleMapper2.class);
    
    job1_2.waitForCompletion(true);
//  ----------------------------------------------------------------------------------
//  ----------------------------------------------------------------------------------
//    
//		Pre Job Operating Part2 - company info refactorying
//
//  ----------------------------------------------------------------------------------
    // Job 이름 설정
 // Job 이름 설정
    Job job2 = new Job(getConf(), "SocietyCompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job2, new Path(INTER_SOCIETYCOMPANY1));

    // Job 클래스 설정
    job2.setJarByClass(WeekToSocietyDriver.class);

    // Reducer 클래스 설정
    job2.setReducerClass(SocietyCompanyReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),	// 11-17-19
      TextInputFormat.class, SocietyCompanyMapper.class);
    
    job2.waitForCompletion(true);
    
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
     Job job2_1 = new Job(getConf(), "SocietyCompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job2_1, new Path(INTER_SOCIETYCOMPANY1_1));

    // Job 클래스 설정
    job2_1.setJarByClass(WeekToSocietyDriver.class);

    // Reducer 클래스 설정
    job2_1.setReducerClass(SocietyCompanyReducer.class);

    job2_1.setMapOutputKeyClass(Text.class);
    job2_1.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job2_1.setInputFormatClass(TextInputFormat.class);
    job2_1.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job2_1.setOutputKeyClass(Text.class);
    job2_1.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job2_1, new Path(otherArgs[2]),	// 10-16-18 = 15-16year
      TextInputFormat.class, SocietyCompanyMapper_sub.class);
    
    job2_1.waitForCompletion(true);
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Job 이름 설정
    Job job3 = new Job(getConf(), "WeekToSocietyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job3, new Path(INTER_SOCIETYCOMPANY2));

    // Job 클래스 설정
    job3.setJarByClass(WeekToSocietyDriver.class);

    // Reducer 클래스 설정
    job3.setReducerClass(SocietyCompanyReducer2.class);

    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job3.setInputFormatClass(TextInputFormat.class);
    job3.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job3, new Path(INTER_SOCIETYCOMPANY1),
      TextInputFormat.class, SocietyCompanyMapper2.class);
    MultipleInputs.addInputPath(job3, new Path(INTER_SOCIETYCOMPANY1_1),
    	      TextInputFormat.class, SocietyCompanyMapper2.class);
    job3.waitForCompletion(true);
//  ----------------------------------------------------------------------------------
//  
//		Main Job Operating Part - combine Major information and company information
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job4 = new Job(getConf(), "WeekToSocietyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job4, new Path(INTER_WTS));

    // Job 클래스 설정
    job4.setJarByClass(WeekToSocietyDriver.class);
    job4.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
    job4.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
    job4.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

    // Reducer 클래스 설정
    job4.setReducerClass(WeekToSocietyReducer.class);

    job4.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
    job4.setMapOutputValueClass(Text.class);
    
    // 입출력 데이터 포맷 설정
    job4.setInputFormatClass(TextInputFormat.class);
    job4.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job4, new Path(INTER_WEEKPEOPLE2),
      TextInputFormat.class, WeekToSocietyMapper1.class);
    MultipleInputs.addInputPath(job4, new Path(INTER_SOCIETYCOMPANY2),
      TextInputFormat.class, WeekToSocietyMapper2.class);
    job4.waitForCompletion(true);
    
    
    
    //return job4.waitForCompletion(true) ? 0:1;
    
    
    
    
 // Job 이름 설정
    Job job5 = new Job(getConf(), "WTSFinalDriver");
    FileOutputFormat.setOutputPath(job5, new Path(args[3]));
 // Job 클래스 설정
    job5.setJarByClass(WeekToSocietyDriver.class);
    // 입출력 데이터 경로 설정
    
  
		// Reducer 클래스 설정
    job5.setReducerClass(WTSFinalReducer.class);
    
    
		job5.setInputFormatClass(TextInputFormat.class);
		
		job5.setOutputFormatClass(TextOutputFormat.class);
    // 출력키 및 출력값 유형 설정
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(IntWritable.class);
		MultipleInputs.addInputPath(job5, new Path(INTER_WTS),
	    	      TextInputFormat.class, WTSFinalMapper.class);
		job5.waitForCompletion(true);
		return job5.waitForCompletion(true) ? 0:1;
    
    //return 0;
  }
}