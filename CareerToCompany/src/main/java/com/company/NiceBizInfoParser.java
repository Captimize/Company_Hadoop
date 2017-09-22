package com.company;

import org.apache.hadoop.io.Text;

public class NiceBizInfoParser {
	private String company;
	private String owner;
	private String location;
	private String identification_number;
	private String size;
	private String categorycode;
  private String start_year;
  private String certificate_year;
  private String code;
  
  //private boolean departureDelayAvailable = true;
  //private boolean distanceAvailable = true;

  //private String uniqueCarrier;

  public NiceBizInfoParser(Text text) {
    try {
    	if(text.toString().contains("#")){
      String[] colums = text.toString().trim().split("#");
      
      if (colums != null && colums.length > 0) {
      // 운항 연도 설정
      //empno = Integer.parseInt(colums[0]);
      company = colums[0];
      owner = colums[1];
      location = colums[2];
      if( colums[2].contains("구")){
    	  //location = colums[2].substring(0, colums[2].indexOf("구"));
      } else{
    	  //location = colums[2];
      }
      //identification_number = colums[3];
      if(colums[3].length()==12){
    	  identification_number = colums[3];
      } else if (colums[3].length()>12){
    	  identification_number = colums[3].substring(colums[3].length()-12, colums[3].length());
      } else {
    	  identification_number = "not exist";
      }
     
      //real_number = colums[3].substring(colums[3].length()-12-1, colums[3].length()-1);
      if(colums[4].contains("폐업")){
    	  size="-";
      }else if(colums[4].contains("비영리단체")){
    	  size = "비영리단체";
      }else if(colums[4].contains(",")){
    	  if(colums[4].contains("대상아님")){
    		  size = "미분류, "+colums[4].substring(colums[4].indexOf(",")+1, colums[4].length());
    	  }else{
    		  size = colums[4].substring(0, colums[4].indexOf(","))+"("+colums[4].substring(colums[4].indexOf(",")+2, colums[4].length())+")";
    		  }
      	//size=colums[4].substring(0, colums[4].indexOf(","));
      		}
      categorycode = colums[5].substring(1,colums[5].indexOf(")"));
      code = colums[5].substring(2,4);
      start_year = colums[6];
      certificate_year = colums[7];
      }
      }
    } catch (Exception e) {
      //System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public NiceBizInfoParser(Text text, int a) {
	    try {
	      String[] colums = text.toString().split("%");
	      String[] next = colums[1].toString().split("&");
	      if (colums != null && colums.length > 0) {
	     
	      code = colums[0];
	      company = next[0];
	      owner = next[1];
	      identification_number = next[2];
	      location = next[3];
	      size = next[4];
	      start_year = next[5];
	      certificate_year = next[6];
	      
	      }
	    } catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }

  public String getcompany() { return company; }

  public String getowner() { return owner; }

  public String getlocation() { return location; }

  public String getidentificationnumber() { return identification_number; }

  public String getsize() { return size;  }
  
  public String getcategorycode() { return categorycode;}
  
  public String getstartyear() { return start_year;}
  
  public String getcertificateyear(){ return certificate_year;}
  
  public String getcode() { return code;}
  
}
