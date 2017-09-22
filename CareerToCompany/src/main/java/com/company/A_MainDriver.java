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


public class A_MainDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Tool 인터페이스 실행
    int res = ToolRunner.run(new Configuration(), new A_MainDriver(), args);
    System.out.println("MR-Job Result:" + res);
  }
  private static final String ABROAD_OUTPUT = "ABROAD_COMPANY_OUTPUT";
  private static final String FUNDING_OUTPUT = "FUNDING_COMPANY_OUTPUT";
  private static final String NICE_OUTPUT = "NICE_COMPANY_OUTPUT";

  private static final String TAX_OUTPUT = "TAX_OUTPUT";
  
  private static final String INTER_WEEKPEOPLE1 = "INTER_WEEKPEOPLE1";
  private static final String INTER_WEEKPEOPLE2 = "INTER_WEEKPEOPLE2";
  private static final String INTER_SOCIETYCOMPANY1 = "INTER_SOCIETYCOMPANY1";
  private static final String INTER_SOCIETYCOMPANY1_1 = "INTER_SOCIETYCOMPANY1_1";
  private static final String INTER_SOCIETYCOMPANY2 = "INTER_SOCIETYCOMPANY2";
  private static final String INTER_WTS = "INTER_WTS";
  private static final String WEEK_TO_SOCIETY_OUTPUT = "WEEK_TO_SOCIETY_OUTPUT";
  
  
  private static final String INTER_MAJORCODE = "INTER_MAJORCODE";
  private static final String INTER_MAJOR = "INTER_MAJOR";
  private static final String INTER_COMPANY = "INTER_COMPANY";
  private static final String MAJOR_TO_NICE_OUTPUT = "MAJOR_TO_NICE_OUTPUT";
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    // 입력출 데이터 경로 확인
    if (otherArgs.length != 8) {
      System.err.println("Usage: CompanyDriver <abroad> <funding> <nice> <tax> <weekpeople> <society11> <society10> <career>");
      ///home/hadoop/abroad /home/hadoop/funding /home/hadoop/nicebizinfo /home/hadoop/tax /home/hadoop/week /home/hadoop/society11 /home/hadoop/society10 /home/hadoop/career
      System.exit(2);
    }
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1 - abroad information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
    // Job 이름 설정
    Job job1 = new Job(getConf(), "CompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job1, new Path(ABROAD_OUTPUT));

    // Job 클래스 설정
    job1.setJarByClass(CompanyDriver.class);


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
    
    
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part2 - funding information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
    // Job 이름 설정
    Job job2 = new Job(getConf(), "CompanyDriver");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job2, new Path(FUNDING_OUTPUT));

    // Job 클래스 설정
    job2.setJarByClass(CompanyDriver.class);


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
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part3 - nice information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
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
    job3.waitForCompletion(true);

//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part4 - Tax information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
			Job job4 = new Job(getConf(), "TaxArrangeDriver");
	 // Job 클래스 설정
			job4.setJarByClass(TaxArrangeDriver.class);
	    // 입출력 데이터 경로 설정
	    
			FileOutputFormat.setOutputPath(job4, new Path(TAX_OUTPUT));

	    
	    
			// Reducer 클래스 설정
			job4.setReducerClass(TaxReducer.class);
	    
	    
			job4.setMapOutputKeyClass(Text.class);
			job4.setMapOutputValueClass(Text.class);
	    // 입출력 데이터 포맷 설정

			job4.setInputFormatClass(TextInputFormat.class);
			
			job4.setOutputFormatClass(TextOutputFormat.class);
	    // 출력키 및 출력값 유형 설정
			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(Text.class);
			MultipleInputs.addInputPath(job4, new Path(otherArgs[3]),
				      TextInputFormat.class, TaxMapper.class);
			job4.waitForCompletion(true);
			
//-------------------------------------------------------------------------------------------------------------------------------
//  
//				Pre Job Operating Part5-1 - WeekPeople1 information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------		
		    // Job 이름 설정
		    Job job5_1 = new Job(getConf(), "WeekToSocietyDriver");

		    // 출력 데이터 경로 설정
		    FileOutputFormat.setOutputPath(job5_1, new Path(INTER_WEEKPEOPLE1));

		    // Job 클래스 설정
		    job5_1.setJarByClass(WeekToSocietyDriver.class);

		    // Reducer 클래스 설정
		    //job1.setReducerClass(SocietyWeekPeopleReducer.class);

		    job5_1.setMapOutputKeyClass(Text.class);
		    job5_1.setMapOutputValueClass(Text.class);

		    // 입출력 데이터 포맷 설정
		    job5_1.setInputFormatClass(TextInputFormat.class);
		    job5_1.setOutputFormatClass(TextOutputFormat.class);

		    // 출력키 및 출력값 유형 설정
		    job5_1.setOutputKeyClass(Text.class);
		    job5_1.setOutputValueClass(Text.class);
		    
		    // MultipleInputs 설정
		    MultipleInputs.addInputPath(job5_1, new Path(otherArgs[4]),
		      TextInputFormat.class, SocietyWeekPeopleMapper.class);
		    
		    job5_1.waitForCompletion(true);
		    
		    
//-------------------------------------------------------------------------------------------------------------------------------
//  
//						Pre Job Operating Part5-2 - WeekPeople2 information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
		    // Job 이름 설정
		    Job job5_2 = new Job(getConf(), "WeekToSocietyDriver");

		    // 출력 데이터 경로 설정
		    FileOutputFormat.setOutputPath(job5_2, new Path(INTER_WEEKPEOPLE2));

		    // Job 클래스 설정
		    job5_2.setJarByClass(WeekToSocietyDriver.class);

		    // Reducer 클래스 설정
		    job5_2.setReducerClass(SocietyWeekPeopleReducer.class);

		    job5_2.setMapOutputKeyClass(Text.class);
		    job5_2.setMapOutputValueClass(Text.class);

		    // 입출력 데이터 포맷 설정
		    job5_2.setInputFormatClass(TextInputFormat.class);
		    job5_2.setOutputFormatClass(TextOutputFormat.class);

		    // 출력키 및 출력값 유형 설정
		    job5_2.setOutputKeyClass(Text.class);
		    job5_2.setOutputValueClass(Text.class);
		    
		    // MultipleInputs 설정
		    MultipleInputs.addInputPath(job5_2, new Path(INTER_WEEKPEOPLE1),
		      TextInputFormat.class, SocietyWeekPeopleMapper2.class);
		    
		    job5_2.waitForCompletion(true);
//-------------------------------------------------------------------------------------------------------------------------------
//  
//			Pre Job Operating Part6-1 - SocietyCompany-1 information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
		    // Job 이름 설정
		 // Job 이름 설정
		    Job job6_1 = new Job(getConf(), "WeekToSocietyDriver");

		    // 출력 데이터 경로 설정
		    FileOutputFormat.setOutputPath(job6_1, new Path(INTER_SOCIETYCOMPANY1));

		    // Job 클래스 설정
		    job6_1.setJarByClass(WeekToSocietyDriver.class);

		    // Reducer 클래스 설정
		    job6_1.setReducerClass(SocietyCompanyReducer.class);

		    job6_1.setMapOutputKeyClass(Text.class);
		    job6_1.setMapOutputValueClass(Text.class);

		    // 입출력 데이터 포맷 설정
		    job6_1.setInputFormatClass(TextInputFormat.class);
		    job6_1.setOutputFormatClass(TextOutputFormat.class);

		    // 출력키 및 출력값 유형 설정
		    job6_1.setOutputKeyClass(Text.class);
		    job6_1.setOutputValueClass(Text.class);
		    
		    // MultipleInputs 설정
		    MultipleInputs.addInputPath(job6_1, new Path(otherArgs[5]),	// 11-17-19
		      TextInputFormat.class, SocietyCompanyMapper.class);
		    
		    job6_1.waitForCompletion(true);
		    
//-------------------------------------------------------------------------------------------------------------------------------
//  
//			Pre Job Operating Part6-2 - SocietyCompany-2 information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
		    
		     Job job6_2 = new Job(getConf(), "WeekToSocietyDriver");

		    // 출력 데이터 경로 설정
		    FileOutputFormat.setOutputPath(job6_2, new Path(INTER_SOCIETYCOMPANY1_1));

		    // Job 클래스 설정
		    job6_2.setJarByClass(WeekToSocietyDriver.class);

		    // Reducer 클래스 설정
		    job6_2.setReducerClass(SocietyCompanyReducer.class);

		    job6_2.setMapOutputKeyClass(Text.class);
		    job6_2.setMapOutputValueClass(Text.class);

		    // 입출력 데이터 포맷 설정
		    job6_2.setInputFormatClass(TextInputFormat.class);
		    job6_2.setOutputFormatClass(TextOutputFormat.class);

		    // 출력키 및 출력값 유형 설정
		    job6_2.setOutputKeyClass(Text.class);
		    job6_2.setOutputValueClass(Text.class);
		    
		    // MultipleInputs 설정
		    MultipleInputs.addInputPath(job6_2, new Path(otherArgs[6]),	// 10-16-18 = 15-16year
		      TextInputFormat.class, SocietyCompanyMapper_sub.class);
		    
		    job6_2.waitForCompletion(true);
//-------------------------------------------------------------------------------------------------------------------------------
//  
//			Pre Job Operating Part6-3 - SocietyCompany-3 information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
		    // Job 이름 설정
		    Job job6_3 = new Job(getConf(), "WeekToSocietyDriver");

		    // 출력 데이터 경로 설정
		    FileOutputFormat.setOutputPath(job6_3, new Path(INTER_SOCIETYCOMPANY2));

		    // Job 클래스 설정
		    job6_3.setJarByClass(WeekToSocietyDriver.class);

		    // Reducer 클래스 설정
		    job6_3.setReducerClass(SocietyCompanyReducer2.class);

		    job6_3.setMapOutputKeyClass(Text.class);
		    job6_3.setMapOutputValueClass(Text.class);

		    // 입출력 데이터 포맷 설정
		    job6_3.setInputFormatClass(TextInputFormat.class);
		    job6_3.setOutputFormatClass(TextOutputFormat.class);

		    // 출력키 및 출력값 유형 설정
		    job6_3.setOutputKeyClass(Text.class);
		    job6_3.setOutputValueClass(Text.class);
		    
		    // MultipleInputs 설정
		    MultipleInputs.addInputPath(job6_3, new Path(INTER_SOCIETYCOMPANY1),
		      TextInputFormat.class, SocietyCompanyMapper2.class);
		    MultipleInputs.addInputPath(job6_3, new Path(INTER_SOCIETYCOMPANY1_1),
		    	      TextInputFormat.class, SocietyCompanyMapper2.class);
		    job6_3.waitForCompletion(true);
//-------------------------------------------------------------------------------------------------------------------------------
//  
//			Pre Job Operating Part7 - WeekToSociety information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
		    // Job 이름 설정
		    Job job7 = new Job(getConf(), "WeekToSocietyDriver");

		    // 출력 데이터 경로 설정
		    FileOutputFormat.setOutputPath(job7, new Path(INTER_WTS));

		    // Job 클래스 설정
		    job7.setJarByClass(WeekToSocietyDriver.class);
		    job7.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
		    job7.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
		    job7.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

		    // Reducer 클래스 설정
		    job7.setReducerClass(WeekToSocietyReducer.class);

		    job7.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
		    job7.setMapOutputValueClass(Text.class);
		    
		    // 입출력 데이터 포맷 설정
		    job7.setInputFormatClass(TextInputFormat.class);
		    job7.setOutputFormatClass(TextOutputFormat.class);

		    // 출력키 및 출력값 유형 설정
		    job7.setOutputKeyClass(Text.class);
		    job7.setOutputValueClass(Text.class);
		    
		    // MultipleInputs 설정
		    MultipleInputs.addInputPath(job7, new Path(INTER_WEEKPEOPLE2),
		      TextInputFormat.class, WeekToSocietyMapper1.class);
		    MultipleInputs.addInputPath(job7, new Path(INTER_SOCIETYCOMPANY2),
		      TextInputFormat.class, WeekToSocietyMapper2.class);
		    job7.waitForCompletion(true);
		    
		    
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part8 - WeekToSociety information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
		    
		 // Job 이름 설정
		    Job job8 = new Job(getConf(), "WeekToSocietyDriver");
		    FileOutputFormat.setOutputPath(job8, new Path(WEEK_TO_SOCIETY_OUTPUT));
		 // Job 클래스 설정
		    job8.setJarByClass(WeekToSocietyDriver.class);
		    // 입출력 데이터 경로 설정
		    
		  
				// Reducer 클래스 설정
		    job8.setReducerClass(WTSFinalReducer.class);
		    
		    
		    job8.setInputFormatClass(TextInputFormat.class);
				
		    job8.setOutputFormatClass(TextOutputFormat.class);
		    // 출력키 및 출력값 유형 설정
		    job8.setOutputKeyClass(Text.class);
		    job8.setOutputValueClass(IntWritable.class);
				MultipleInputs.addInputPath(job8, new Path(INTER_WTS),
			    	      TextInputFormat.class, WTSFinalMapper.class);
				job8.waitForCompletion(true);			
//-------------------------------------------------------------------------------------------------------------------------------
//  
//				Pre Job Operating Part9-1 - MajorToCode information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
				// Job 이름 설정
			    Job job9_1 = new Job(getConf(), "MajorToNiceDriver");

			    // 출력 데이터 경로 설정
			    FileOutputFormat.setOutputPath(job9_1, new Path(INTER_MAJORCODE));

			    // Job 클래스 설정
			    job9_1.setJarByClass(MajorToNiceDriver.class);

			    // Reducer 클래스 설정
			    //job1_0.setReducerClass(MajorToCodeReducer.class);


			    // 입출력 데이터 포맷 설정
			    job9_1.setInputFormatClass(TextInputFormat.class);
			    job9_1.setOutputFormatClass(TextOutputFormat.class);

			    // 출력키 및 출력값 유형 설정
			    job9_1.setOutputKeyClass(Text.class);
			    job9_1.setOutputValueClass(Text.class);
			    
			    // MultipleInputs 설정
			    MultipleInputs.addInputPath(job9_1, new Path(otherArgs[7]),
			      TextInputFormat.class, MajorToCodeMapper.class);
			    
			    job9_1.waitForCompletion(true);
//-------------------------------------------------------------------------------------------------------------------------------
//  
//			Pre Job Operating Part9-2 - MajorToCode information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
			    // Job 이름 설정
			    Job job9_2 = new Job(getConf(), "MajorToNiceDriver");

			    // 출력 데이터 경로 설정
			    FileOutputFormat.setOutputPath(job9_2, new Path(INTER_MAJOR));

			    // Job 클래스 설정
			    job9_2.setJarByClass(MajorToNiceDriver.class);

			    // Reducer 클래스 설정
			    job9_2.setReducerClass(MajorToNicePreReducer1.class);

			    job9_2.setMapOutputKeyClass(Text.class);
			    job9_2.setMapOutputValueClass(Text.class);

			    // 입출력 데이터 포맷 설정
			    job9_2.setInputFormatClass(TextInputFormat.class);
			    job9_2.setOutputFormatClass(TextOutputFormat.class);

			    // 출력키 및 출력값 유형 설정
			    job9_2.setOutputKeyClass(Text.class);
			    job9_2.setOutputValueClass(Text.class);
			    
			    // MultipleInputs 설정
			    MultipleInputs.addInputPath(job9_2, new Path(INTER_MAJORCODE),
			      TextInputFormat.class, MajorMapper.class);
			    
			    job9_2.waitForCompletion(true);
//-------------------------------------------------------------------------------------------------------------------------------
//  
//			Pre Job Operating Part10 - Company information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
			    // Job 이름 설정
			    Job job10 = new Job(getConf(), "MajorToNiceDriver");

			    // 출력 데이터 경로 설정
			    FileOutputFormat.setOutputPath(job10, new Path(INTER_COMPANY));

			    // Job 클래스 설정
			    job10.setJarByClass(MajorToNiceDriver.class);
			    job10.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
			    job10.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
			    job10.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

			    // Reducer 클래스 설정
			    job10.setReducerClass(MajorToNicePreReducer2.class);

			    job10.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
			    job10.setMapOutputValueClass(Text.class);

			    // 입출력 데이터 포맷 설정
			    job10.setInputFormatClass(TextInputFormat.class);
			    job10.setOutputFormatClass(TextOutputFormat.class);

			    // 출력키 및 출력값 유형 설정
			    job10.setOutputKeyClass(Text.class);
			    job10.setOutputValueClass(Text.class);
			    
			    // MultipleInputs 설정
			    MultipleInputs.addInputPath(job10, new Path(otherArgs[2]),
			      TextInputFormat.class, NiceMapper.class);

			    job10.waitForCompletion(true);
//-------------------------------------------------------------------------------------------------------------------------------
//  
//						Pre Job Operating Part11 - MajorToCode information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
			    // Job 이름 설정
			    Job job11 = new Job(getConf(), "MajorToNiceDriver");

			    // 출력 데이터 경로 설정
			    FileOutputFormat.setOutputPath(job11, new Path(MAJOR_TO_NICE_OUTPUT));

			    // Job 클래스 설정
			    job11.setJarByClass(MajorToNiceDriver.class);
			    job11.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
			    job11.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
			    job11.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

			    // Reducer 클래스 설정
			    job11.setReducerClass(MajorToNiceReducer.class);

			    job11.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
			    job11.setMapOutputValueClass(Text.class);
			    
			    // 입출력 데이터 포맷 설정
			    job11.setInputFormatClass(TextInputFormat.class);
			    job11.setOutputFormatClass(TextOutputFormat.class);

			    // 출력키 및 출력값 유형 설정
			    job11.setOutputKeyClass(Text.class);
			    job11.setOutputValueClass(Text.class);
			    
			    // MultipleInputs 설정
			    MultipleInputs.addInputPath(job11, new Path(INTER_MAJOR),
			      TextInputFormat.class, MajorToNiceMapper1.class);
			    MultipleInputs.addInputPath(job11, new Path(INTER_COMPANY),
			      TextInputFormat.class, MajorToNiceMapper2.class);
			
			    return job11.waitForCompletion(true) ? 0:1;
			
			
  }
}