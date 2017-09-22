package com.company;

import org.apache.hadoop.io.Text;

public class SocietyCompanyInfoParser {
	private String goods;
  private String company_name;
  private String company_location;
  private String detail;
  //private boolean departureDelayAvailable = true;
  //private boolean distanceAvailable = true;

  //private String uniqueCarrier;

  public SocietyCompanyInfoParser(Text text) {
    try {
      String[] colums = text.toString().split("\t");
      if (colums != null && colums.length > 0) {
      company_name = colums[11];
      if(colums[17].contains("시")){
    	  company_location = colums[17].substring(0, colums[17].lastIndexOf("시")+1);
    	  //company_location = "zz";
      }else if(colums[17].contains("군")){
    	  company_location = colums[17].substring(0, colums[17].lastIndexOf("군")+1);
      }
      if(colums.length==20){
    	  goods = colums[19];
      }else{
    	  goods="NULL";
      }
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public SocietyCompanyInfoParser(Text text, int a) {
	    try {
	      String[] colums = text.toString().split("%");
	      if (colums != null && colums.length > 0) {
	    	  company_name = colums[0];
	    	  String[] sub_colums = colums[1].toString().split("&");
	    	  company_location = sub_colums[0];
	    	  goods = sub_colums[1];
	      		}
	    } catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }
  public String getgoods() { return goods;}
  
  public String getcompanyname(){ return company_name;}
  
  public String getcompanylocation(){ return company_location;}
  public String getcompanylocation2() { 
	  
		  if(company_location.contains("여주")){
			  company_location="경기도 여주시";
		  }else if(company_location.contains("포천")){
			  company_location="경기도 포천시";
		  }else if(company_location.contains("마산")){
			  company_location="경상남도 창원시";
		  }else if(company_location.contains("진해")){
			  company_location="경상남도 창원시";
		  }else if(company_location.contains("울릉")){
			  company_location="경상북도 울진군";
		  }else if(company_location.contains("당진")){
			  company_location="충청남도 당진시";
		  }else if(company_location.contains("청원")){
			  company_location="충청북도 청주시";
		  }else if(company_location.contains("양주")){
			  company_location="경기도 양주시";
		  }
	  return company_location;
	  }
  
  public String getdetail() { return detail;}
  public SocietyCompanyInfoParser(Text text, int a,int b) {
	    try {
	      String[] colums = text.toString().split("%");
	      if (colums != null && colums.length > 0) {
	    	  company_location = colums[0];
	    	  detail = colums[1];
	      		}
	    } catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }
  public SocietyCompanyInfoParser(Text text,Boolean a) {
	    try {
	      String[] colums = text.toString().split("\t");
	      if (colums != null && colums.length > 0) {
	      company_name = colums[10];
	      if(colums[16].contains("시")){
	    	  company_location = colums[16].substring(0, colums[16].lastIndexOf("시")+1);
	    	  //company_location = "zz";
	      }else if(colums[16].contains("군")){
	    	  company_location = colums[16].substring(0, colums[16].lastIndexOf("군")+1);
	      }
	      if(colums.length==19){
	    	  goods = colums[18];
	      }else{
	    	  goods="NULL";
	      }
	      }
	    } catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }
}
