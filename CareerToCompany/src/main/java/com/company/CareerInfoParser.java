/*package com.company;

import org.apache.hadoop.io.Text;

public class CareerInfoParser {
	private String major;
	private String detail=null;
  //private boolean departureDelayAvailable = true;
  //private boolean distanceAvailable = true;

  //private String uniqueCarrier;

  public CareerInfoParser(Text text) {
    try {
      String[] colums = text.toString().split("#");
      if (colums != null && colums.length > 0) {
      // 운항 연도 설정
      //empno = Integer.parseInt(colums[0]);
      major = colums[0];
      if(colums[1].contains("고분자")){
    	  detail+="#B03";
      }
      //detail = colums[1];
      }
      
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }

  public String getmajor() { return major; }

  public String getdetail() { return detail; }
  

  
}
*/