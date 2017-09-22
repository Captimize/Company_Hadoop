package com.company;

import org.apache.hadoop.io.Text;

public class SocietyWeekPeopleInfoParser {
	private String age;
	private String sex;
	private String category;
	private String detail;
	
	private String weekday="";
	
	//private String weekend="";
  //private boolean departureDelayAvailable = true;
  //private boolean distanceAvailable = true;

  //private String uniqueCarrier;

  public SocietyWeekPeopleInfoParser(Text text) {
    try {
      String[] colums = text.toString().split("\t");
      if (colums != null && colums.length > 0) {
    	  age = colums[0].substring(1,3).trim();
    	  if(age.contains("[")){
    		  age=colums[0].substring(2, 4).trim();
    	  		}
    	  sex = colums[1].trim();
    	  if(colums[2].equalsIgnoreCase("A")){
    		  category = "1";
    	  }else if (colums[2].equalsIgnoreCase("B")){
    		  category = "2";
    	  }else{
    		  category = "3";
    	  		}
      
    	  if(!colums[3].equalsIgnoreCase("#")){
    		  
    		  weekday = mapping(colums[3])+" "+colums[4]+"#";}
    	  for(int k=6;k<37;k+=3){
    		  if(!colums[k].equalsIgnoreCase("#")){weekday += mapping(colums[k])+" "+colums[k+1]+"#";}
      			}
      		}
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public SocietyWeekPeopleInfoParser(Text text, int a) {
	    try {
	    	if(text.toString().contains("%")){
	    		String[] columss = text.toString().split("%");
	    		weekday = columss[0];
	  	    detail = columss[1];
	    		}
	    	
	      
	    	}
	     catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }
  public String getage() { return age; }
  public String getsex() { return sex; }
  public String getcategory() { return category; }
  public String getweekday() { 
	  if(!weekday.contains("시")&&!weekday.contains("구")){
		  return null;
	  }else if(weekday.contains("시")&&weekday.contains("구")){
		  weekday = weekday.substring(0,weekday.indexOf("시")+1);
		  
	  	}
	  return weekday; 
	}
  public String getdetail() { return detail;}
  public String mapping(String a){
	  if(a.contains("경기")){
		  a="경기도";
	  }else if(a.contains("강원")){
		  a="강원도";
	  }else if(a.contains("경남")){
		  a="경상남도";
	  }else if(a.contains("경북")){
		  a="경상북도";
	  }else if(a.contains("광주")){
		  a="광주광역시";
	  }else if(a.contains("대구")){
		  a="대구광역시";
	  }else if(a.contains("대전")){
		  a="대전광역시";
	  }else if(a.contains("부산")){
		  a="부산광역시";
	  }else if(a.contains("서울")){
		  a="서울특별시";
	  }else if(a.contains("세종")){
		  a="세종특별자치시";
	  }else if(a.contains("울산")){
		  a="울산광역시";
	  }else if(a.contains("인천")){
		  a="인천광역시";
	  }else if(a.contains("전남")){
		  a="전라남도";
	  }else if(a.contains("전북")){
		  a="전라북도";
	  }else if(a.contains("제주")){
		  a="제주특별자치도";
	  }else if(a.contains("충남")){
		  a="충청남도";
	  }else if(a.contains("충북")){
		  a="충청북도";
	  }
	  
	  return a;
  	}
  public String submapping(String a){
	  if(a.contains("여주")){
		  a="여주시";
	  }else if(a.contains("포천")){
		  a="포천시";
	  }else if(a.contains("마산")){
		  a="창원시";
	  }else if(a.contains("진해")){
		  a="창원시";
	  }else if(a.contains("울릉")){
		  a="울진군";
	  }else if(a.contains("당진")){
		  a="당진시";
	  }else if(a.contains("청원")){
		  a="청주시";
	  }
	  return a;
  	}
  public String getweekday2() {
	  return weekday; 
	}
}
