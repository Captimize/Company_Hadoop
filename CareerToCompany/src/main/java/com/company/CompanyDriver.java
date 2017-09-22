package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mapsidejoin.TaggedGroupKeyComparator;


class CompanyDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Tool 인터페이스 실행
    int res = ToolRunner.run(new Configuration(), new CompanyDriver(), args);
    System.out.println("MR-Job Result:" + res);
  }
  private static final String ABROAD_OUTPUT = "ABROAD_COMPANY_OUTPUT";
  private static final String FUNDING_OUTPUT = "FUNDING_COMPANY_OUTPUT";
  private static final String NICE_OUTPUT = "NICE_COMPANY_OUTPUT";
  private static final String SOCIETY_OUTPUT10 = "SOCIETY_COMPANY_OUTPUT1";
  private static final String SOCIETY_OUTPUT11 = "SOCIETY_COMPANY_OUTPUT2";
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    // 입력출 데이터 경로 확인
    if (otherArgs.length != 3) {
      System.err.println("Usage: CompanyDriver <abroad> <funding> <nice>");
      System.exit(2);
    }
//  ----------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1 - abroad information refactorying
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job1 = new Job(getConf(), "CompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job1, new Path(ABROAD_OUTPUT));

    // Job 클래스 설정
    job1.setJarByClass(CompanyDriver.class);

    // Reducer 클래스 설정
    //job1.setReducerClass(SocietyWeekPeopleReducer.class);


    // 입출력 데이터 포맷 설정
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job1, new Path(otherArgs[0]),
      TextInputFormat.class, AbroadMapper.class);
    
    job1.waitForCompletion(true);
    
    
//  ----------------------------------------------------------------------------------
//  
//		Pre Job Operating Part2 - funding information refactorying
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job2 = new Job(getConf(), "CompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job2, new Path(FUNDING_OUTPUT));

    // Job 클래스 설정
    job2.setJarByClass(CompanyDriver.class);

    // Reducer 클래스 설정
    //job2.setReducerClass(SocietyWeekPeopleReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),
      TextInputFormat.class, FundingMapper.class);
    
    job2.waitForCompletion(true);
//  ----------------------------------------------------------------------------------
//  
//		Pre Job Operating Part3 - nice information refactorying
//
//----------------------------------------------------------------------------------
    // Job 이름 설정
    Job job3 = new Job(getConf(), "CompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job3, new Path(NICE_OUTPUT));

    // Job 클래스 설정
    job3.setJarByClass(MajorToNiceDriver.class);
    job3.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
    job3.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
    job3.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

    // Reducer 클래스 설정
    job3.setReducerClass(MajorToNicePreReducer2.class);

    job3.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
    job3.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job3.setInputFormatClass(TextInputFormat.class);
    job3.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job3, new Path(otherArgs[2]),
      TextInputFormat.class, NiceDefaultMapper.class);
    return job3.waitForCompletion(true) ? 0:1;
    //job3.waitForCompletion(true);
//  ----------------------------------------------------------------------------------
//    
//		Pre Job Operating Part4-1 - society information refactorying
//
//  ----------------------------------------------------------------------------------
/*    // Job 이름 설정
 // Job 이름 설정
    Job job4_1 = new Job(getConf(), "SocietyCompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job4_1, new Path(SOCIETY_OUTPUT10));

    // Job 클래스 설정
    job4_1.setJarByClass(CompanyDriver.class);

    // Reducer 클래스 설정
    job4_1.setReducerClass(SocietyCompanyReducer.class);

    job4_1.setMapOutputKeyClass(Text.class);
    job4_1.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job4_1.setInputFormatClass(TextInputFormat.class);
    job4_1.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job4_1.setOutputKeyClass(Text.class);
    job4_1.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job4_1, new Path(otherArgs[3]),	// 11-17-19
      TextInputFormat.class, SocietyCompanyMapper.class);
    
    job4_1.waitForCompletion(true);
    
//  ----------------------------------------------------------------------------------
//  
//		Pre Job Operating Part4-2 - society information refactorying
//
//----------------------------------------------------------------------------------
    
     Job job4_2 = new Job(getConf(), "SocietyCompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job4_2, new Path(SOCIETY_OUTPUT11));

    // Job 클래스 설정
    job4_2.setJarByClass(CompanyDriver.class);

    // Reducer 클래스 설정
    job4_2.setReducerClass(SocietyCompanyReducer.class);

    job4_2.setMapOutputKeyClass(Text.class);
    job4_2.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job4_2.setInputFormatClass(TextInputFormat.class);
    job4_2.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job4_2.setOutputKeyClass(Text.class);
    job4_2.setOutputValueClass(Text.class);
    
    // MultipleInputs 설정
    MultipleInputs.addInputPath(job4_2, new Path(otherArgs[4]),	// 10-16-18 = 15-16year
      TextInputFormat.class, SocietyCompanyMapper_sub.class);
    
    //job4_2.waitForCompletion(true);
*/
		//return job4_2.waitForCompletion(true) ? 0:1;
    
    //return 0;
  }
}