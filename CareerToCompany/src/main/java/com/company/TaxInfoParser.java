package com.company;

import java.util.HashMap;

import org.apache.hadoop.io.Text;

public class TaxInfoParser {
	private String category;
	private String categorycode;
	private String work_day;
	private String total_pay;
	private String age;
	private String size;
	private String sex;
  //private boolean departureDelayAvailable = true;
  //private boolean distanceAvailable = true;

  //private String uniqueCarrier;

  public TaxInfoParser(Text text) {
    try {
      String[] colums = text.toString().trim().split("#");
      if (colums != null && colums.length > 0) {
      
      //category = colums[1].charAt(0)+"$"+colums[1].charAt(1);
      //category = categorying.get(colums[1]);
      categorycode = colums[1];
      
      
    	  if (colums[3].equalsIgnoreCase("1")){
    		  size = "7";
    	  }else if(colums[3].equalsIgnoreCase("2")){
    		  size = "20";
    	  }else if(colums[3].equalsIgnoreCase("3")){
    		  size = "65";
    	  }else if(colums[3].equalsIgnoreCase("4")){
    		  size = "200";
    	  }else if (colums[3].equalsIgnoreCase("5")){
    		  size = "400";
    	  }else if (colums[3].equalsIgnoreCase("6")){
    		  size = "1000";
    	  		}
      
      sex = colums[4];
      
      //category = colums[1];
      
      work_day = colums[11];
      total_pay = colums[15];
      age = colums[6];
      
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public String getcategorycode(){return categorycode;}
  public String getcategory() { return category; }
  public String getage() { return age; }
  public String getworkday() { return work_day; }
  public String gettotalpay() { return total_pay; }
  public String getsex() { return sex;}
  public String getsize() { return size;}
  
  
}
