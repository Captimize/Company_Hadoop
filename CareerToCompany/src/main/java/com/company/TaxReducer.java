
package com.company;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxReducer extends Reducer<Text, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();
  private String[] temp;
  private String categoryname;
  private float size=0;
  private int sizecount=0;
  private float sex=0;
  private int sexcount=0;
  private float work_day=0;
  private int work_daycount=0;
  private float total_pay=0;
  private int total_paycount=0;
  private float age = 0;
  private int agecount=0;
  private String pattern2=".##";
  DecimalFormat dformat = new DecimalFormat(pattern2);
  private int number_of_worker=0;
  HashMap<String,String> categorying = new HashMap();
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    outputKey.set(key+"&");
    
    Text info = new Text(iterator.next());	// input[0] 
    temp = info.toString().split("#");
    if(!(temp[0].equalsIgnoreCase("null"))){
    	size = Integer.parseInt(temp[0].trim());
  	  sizecount++;
    	}
    if(!(temp[1].equalsIgnoreCase("null"))){
    	sex = Integer.parseInt(temp[1].trim());
  	  sexcount++;
    	}
    if(!(temp[2].equalsIgnoreCase("null"))){
    	age = Integer.parseInt(temp[2].trim());
  	  agecount++;
    	}
    if(!(temp[3].equalsIgnoreCase("null"))){
    	total_pay = Integer.parseInt(temp[3].trim());
  	  total_paycount++;
    	}
    if(!(temp[4].equalsIgnoreCase("null"))){
    	work_day = Integer.parseInt(temp[4].trim());
  	  work_daycount++;
    	}

    number_of_worker++;
    	//outputValue.set(outputValue.toString()+info.toString());
        while (iterator.hasNext()) {
          Text record = iterator.next();							// input[1]
          temp = record.toString().split("#");
          if(!(temp[0].equalsIgnoreCase("null"))){
        	  size += Integer.parseInt(temp[0].trim());
        	  sizecount++;
          			}
          if(!(temp[1].equalsIgnoreCase("null"))){
        	  sex += Integer.parseInt(temp[1].trim());
        	  sexcount++;
          			}
          if(!(temp[2].equalsIgnoreCase("null"))){
        	  age += Integer.parseInt(temp[2].trim());
        	  agecount++;
          			}
          if(!(temp[3].equalsIgnoreCase("null"))){
        	  total_pay += Integer.parseInt(temp[3].trim());
        	  total_paycount++;
          			}
          if(!(temp[4].equalsIgnoreCase("null"))){
        	  work_day += Integer.parseInt(temp[4].trim());
        	  work_daycount++;
          			}
          
          
          number_of_worker++;
          
        		}
        
        size = size / sizecount;
        sex = sex / sexcount -1;
        age = age / agecount;
        total_pay = total_pay / total_paycount;
        work_day = work_day / work_daycount;
        MappingCategory();
        categoryname = categorying.get(key.toString());
        outputValue.set(categoryname+"&"+dformat.format(size)+"&"+dformat.format(sex)+"&"+dformat.format(age)+"&"+dformat.format(total_pay)+"&"+dformat.format(work_day)+"&"+number_of_worker);
        context.write(outputKey, outputValue);
        outputValue.set("");
        size = 0;
        sizecount=0;
        sex=0;
        sexcount=0;
        age = 0;
        agecount=0;
        total_pay=0;
        total_paycount=0;
        work_day=0;
        work_daycount=0;
        number_of_worker=0;
        
  }
  
  
  
public void MappingCategory(){
	  
	  categorying.put("01", "농업");
	  categorying.put("02", "임업");
	  categorying.put("03", "어업");
	  categorying.put("05", "석탄, 원유 및 천연가스 광업");
	  categorying.put("06", "금속 광업");
	  categorying.put("07", "비금속광물 광업");
	  categorying.put("08", "광업 지원 서비스업");
	  
	  categorying.put("10", "식료품 제조업");
	  categorying.put("11", "음료 제조업");
	  categorying.put("12", "담배 제조업");
	  categorying.put("13", "섬유제품 제조업");
	  categorying.put("14", "의복, 의복액세서리 및 모피제품 제조업");
	  categorying.put("15", "가죽, 가방 및 신발 제조업");
	  categorying.put("16", "목재 및 나무제품 제조업");
	  categorying.put("17", "펄프, 종이 및 종이제품 제조업");
	  
	  categorying.put("18", "인쇄 및 기록매체 복제업");
	  categorying.put("19", "코크스, 연탄 및 석유정제품 제조업");
	  categorying.put("20", "화학물질 및 화학제품 제조업");
	  categorying.put("21", "의료용 물질 및 의약품 제조업");
	  categorying.put("22", "고무제품 및 플라스틱제품 제조업");
	  categorying.put("23", "비금속 광물제품 제조업");
	  categorying.put("24", "1차 금속 제조업");
	  categorying.put("25", "금속가공제품 제조업");
	  
	  categorying.put("26", "전자부품, 컴퓨터, 영상, 음향 및 통신장비 제조업");
	  categorying.put("27", "의료, 정밀, 광학기기 및 시계 제조업");
	  categorying.put("28", "전기장비 제조업");
	  categorying.put("29", "기타 기계 및 장비 제조업");
	  categorying.put("30", "자동차 및 트레일러 제조업");
	  categorying.put("31", "기타 운송장비 제조업");
	  categorying.put("32", "가구 제조업");
	  categorying.put("33", "기타 제품 제조업");
	  
	  categorying.put("35", "전기, 가스, 증기 및 공기조절 공급업");
	  categorying.put("36", "수도사업");
	  categorying.put("37", "하수, 폐수 및 분뇨 처리업");
	  categorying.put("38", "폐기물 수집운반, 처리 및 원료재생업");
	  categorying.put("39", "환경 정화 및 복원업");
	  categorying.put("41", "종합 건설업");
	  categorying.put("42", "전문직별 공사업");
	  categorying.put("45", "자동차 및 부품 판매업");
	  categorying.put("46", "도매 및 상품중개업");
	  
	  categorying.put("47", "소매업");
	  categorying.put("49", "육상운송 및 파이프라인 운송업");
	  categorying.put("50", "수상 운송업");
	  categorying.put("51", "항공 운송업");
	  categorying.put("52", "창고 및 운송관련 서비스업");
	  categorying.put("55", "숙박업");
	  categorying.put("56", "음식점 및 주점업");
	  categorying.put("58", "출판업");
	  
	  categorying.put("59", "영상·오디오 기록물 제작 및 배급업");
	  categorying.put("60", "방송업");
	  categorying.put("61", "통신업");
	  categorying.put("62", "컴퓨터 프로그래밍, 시스템 통합 및 관리업");
	  categorying.put("63", "정보서비스업");
	  categorying.put("64", "금융업");
	  categorying.put("65", "보험 및 연금업");
	  categorying.put("66", "금융 및 보험 관련 서비스업");
	  
	  categorying.put("68", "부동산업");
	  categorying.put("69", "임대업");
	  categorying.put("70", "연구개발업");
	  categorying.put("71", "전문서비스업");
	  categorying.put("72", "건축기술, 엔지니어링 및 기타 과학기술 서비스업");
	  categorying.put("73", "기타 전문, 과학 및 기술 서비스업");
	  categorying.put("74", "사업시설 관리 및 조경 서비스업");
	  categorying.put("75", "사업지원 서비스업");
	  
	  categorying.put("84", "임대업");
	  categorying.put("85", "공공행정, 국방 및 사회보장 행정");
	  categorying.put("86", "교육 서비스업");
	  categorying.put("87", "보건업");
	  categorying.put("88", "사회복지 서비스업");
	  categorying.put("90", "창작, 예술 및 여가관련 서비스업");
	  categorying.put("91", "스포츠 및 오락관련 서비스업");
	  
	  categorying.put("94", "협회 및 단체");
	  categorying.put("95", "수리업");
	  categorying.put("96", "기타 개인 서비스업");
	  categorying.put("97", "가구내 고용활동");
	  categorying.put("98", "달리 분류되지 않은 자가소비를 위한 가구의 재화 및 서비스 생산활동");
	  categorying.put("99", "국제 및 외국기관");
  }
}




