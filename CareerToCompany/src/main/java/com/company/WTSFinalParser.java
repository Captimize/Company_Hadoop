package com.company;

import org.apache.hadoop.io.Text;

public class WTSFinalParser {
	private String region;
	//private String company;
	private String statistics;

  public WTSFinalParser(Text text) {
    try {
      String[] colums = text.toString().trim().split("%");
      if (colums != null && colums.length > 0) {
      if(colums[0].contains("양주군")||colums[0].contains("여주군")||colums[0].contains("포천군")||colums[0].contains("마산시")||colums[0].contains("진해시")||colums[0].contains("당진군")||colums[0].contains("청원군")){
    	  region = colums[0];
    	  statistics = colums[3];
      }else{
    	  region = colums[0];
    	  statistics = colums[2];
      }
      
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public String getregion(){return region;}
  public String getstatistics() { return statistics; }
  
  
}
