package com.company;

import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class MajorInfoParser {
	private String major;
	private String test;
	private String[] code=null;
	private int count=0;
	private String[] colums;
  
  //private boolean departureDelayAvailable = true;
  //private boolean distanceAvailable = true;

  //private String uniqueCarrier;

  public MajorInfoParser(Text text) {
    try {
    	if(text.toString().contains("#")){
      colums = text.toString().split("#");
    	}
      
      if (colums[1] != null ) {
      // 운항 연도 설정
      //empno = Integer.parseInt(colums[0]);
    	  major = colums[0].trim();
    	  test = colums[1].trim();
    	  //code = colums[1].substring(1,4);
    	  if(!test.contains(",")){
    		  code = test.split("]");
    		  //code[0] = code[0].substring(1, 3);
    		  setcount(1);
    	  }else if (test.lastIndexOf(",")==3){
    		  code = test.split(",");
    		  setcount(2);
    	  }else if ( test.lastIndexOf(",")==7){
    		  code = test.split(",");
    		  setcount(3);
    	  }else if ( test.lastIndexOf(",")==11){
    		  code = test.split(",");
    		  setcount(4);
    	  }else if ( test.lastIndexOf(",")==15){
    		  code = test.split(",");
    		  setcount(5);
    	  }else if ( test.lastIndexOf(",")==19){
    		  code = test.split(",");
    		  setcount(6);
    	  }else if ( test.lastIndexOf(",")==23){
    		  code = test.split(",");
    		  setcount(7);
    	  }else if ( test.lastIndexOf(",")==27){
    		  code = test.split(",");
    		  setcount(8);
    	  }else if ( test.lastIndexOf(",")==31){
    		  code = test.split(",");
    		  setcount(9);
    	  }else if ( test.lastIndexOf(",")==35){
    		  code = test.split(",");
    		  setcount(9);
    	  }else if ( test.lastIndexOf(",")==39){
    		  code = test.split(",");
    		  setcount(9);
    	  }else if ( test.lastIndexOf(",")==43){
    		  code = test.split(",");
    		  setcount(9);
    	  }else if ( test.lastIndexOf(",")==47){
    		  code = test.split(",");
    		  setcount(9);
    	  }else if ( test.lastIndexOf(",")==51){
    		  code = test.split(",");
    		  setcount(9);
    	  }else if ( test.lastIndexOf(",")==55){
    		  code = test.split(",");
    		  setcount(9);
    	  }
    	  
    	  
      		}	
    	}
     catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public MajorInfoParser(Text text, int a) {
	    try {
	    	if(text.toString().contains("%")){
	      colums = text.toString().split("%");
	    	}
	    	test = colums[0];
	    	major = colums[1];
	      
	    	}
	     catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }

  public String getmajor() { return major; }

  public String getcode(int a) { 
	  return code[a].substring(1, 3); 
  }
  
  public int getcount() { return count;}
  public void setcount(int count) { this.count = count;}
  public String gettest() { return test;}
  
}