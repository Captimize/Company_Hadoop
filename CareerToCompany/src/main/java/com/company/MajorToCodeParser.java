package com.company;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

public class MajorToCodeParser {
   private String major;
   private String detail;
   private ArrayList<String> ar = new ArrayList<String>();
   private String select;
   private String[] code;
   private int count = 0;
   private int i = 0;
   // private boolean departureDelayAvailable = true;
   // private boolean distanceAvailable = true;

   // private String uniqueCarrier;

   public MajorToCodeParser(Text text) {
      try {
         String[] colums = text.toString().split("#");

         // 운항 연도 설정
         // empno = Integer.parseInt(colums[0]);
         major = colums[0];

         if (colums[1].contains("농업") || colums[1].contains("작물") || colums[1].contains("재배")) {
            ar.add("01");
            if (colums[1].contains("기계") || colums[1].contains("동물") || colums[1].contains("문예") || colums[1].contains("시각디자인") || colums[1].contains("작곡과")){
               ar.remove("01");
            }
         }
         if (colums[1].contains("임업")) {
            ar.add("02");
         }
         if (colums[1].contains(" 어업")) {
            ar.add("03");
         }
      
         if (colums[1].contains("석탄")) {
            ar.add("05");
         }
         if (colums[1].contains("식품영양") || colums[1].contains("도축") || colums[1].contains("식품가공")) {
            ar.add("10");
         }
         if (colums[1].contains("음료")) {
            ar.add("11");
         }
         if (colums[1].contains("섬유") || colums[1].contains("직물")) {
            ar.add("13");
         }
         if (colums[1].contains("의류") || colums[1].contains("의복")) {
            ar.add("14");
         }
         if (colums[1].contains("신발제조") || colums[1].contains("구두") || colums[1].contains(" 가방")) {
            ar.add("15");
         }
         if (colums[1].contains("목재")) {
            ar.add("16");
         }
         if(colums[1].contains("제지 공학") || colums[1].contains("펄프"))
      {
          ar.add("17");
          ar.add("18");
      }
      
      if(colums[1].contains("인쇄"))
          ar.add("18"); 
      if(colums[1].contains("화학공학") || colums[1].contains("화학 공학"))
          ar.add("19");       
         if (colums[1].contains("제조")) {
            if (colums[1].contains("화학"))
               ar.add("20");
         }
         if (colums[1].contains("고분자공") || colums[1].contains("감광")) {
            if (ar.contains("20"))
               ;
            else
               ar.add("20");
         }
         if (colums[1].contains("의약품")) {
            ar.add("21");
            if (colums[1].contains("방사선") || colums[1].contains("품질관리"))
               ar.remove("21");
         }
         if (colums[1].contains("플라스틱") && colums[1].contains("제조")) {
            ar.add("22");
         }
         if ((colums[1].contains("유리") && colums[1].contains("가공")) || colums[1].contains("도자기")
               || colums[1].contains("비금속")) {
            ar.add("23");
            if (colums[1].contains("자동차"))
               ar.remove("23");
         }

         if (colums[1].contains("제철") || colums[1].contains("제련") || colums[1].contains("금속제조")
               || colums[1].contains("금속 제조")) {
            ar.add("24");
         }
         if (colums[1].contains("금속제조")) {
            ar.add("25");
         }
         if (colums[1].contains("반도체") || colums[1].contains("통신장비")) {
            ar.add("26");
         }
         if (colums[1].contains("의료기기") || colums[1].contains("치기공") || colums[1].contains("광학기기")) {
            ar.add("27");
         }
         if (colums[1].contains("제조") && colums[1].contains("기계") || colums[1].contains("제어계측")
               || colums[1].contains("로봇")) {
            ar.add("29");
         }
         if (colums[1].contains("자동차엔진") || colums[1].contains("자동차 엔진") || colums[1].contains("자동차 부품")) {
            ar.add("30");
         }
         if ((colums[1].contains("조선") && colums[1].contains(" 기계"))
               || ((colums[1].contains("항공기") && colums[1].contains(" 기계")))) {
            ar.add("31");
         }
         if (colums[1].contains("가구") && colums[1].contains("제조")) {
            ar.add("32");
         }
         if (colums[1].contains("귀금속") || (colums[1].contains("악기") && colums[1].contains("제조"))
               || colums[1].contains("광고물")) {
            ar.add("33");
         }
         if (colums[1].contains("기계") && colums[1].contains("수리")
               || (colums[1].contains("기기")) && colums[1].contains("수리")) {
            ar.add("34");
         }
         if (colums[1].contains("전기공학") || colums[1].contains("발전기") || colums[1].contains("화력발전")
               || colums[1].contains("수력발전") || colums[1].contains("송배전")) {
            if (colums[1].contains("농수산") || colums[1].contains("물리") || colums[1].contains("소방"))
               ;
            else
               ar.add("35");
         }
         if (colums[1].contains("상수도")) {
            ar.add("36");
         }
         if (colums[1].contains("폐수")) {
            ar.add("37");
         }
         if (colums[1].contains("폐기물")) {
            ar.add("38");
         }
         if (colums[1].contains("환경정화") && colums[1].contains("환경보건")) {
            ar.add("39");
         }
         if (colums[1].contains("건축 ") || colums[1].contains("토목") || colums[1].contains("철도건설")
               || colums[1].contains("조경설계")) {
            ar.add("41");

         }
         if (colums[1].contains("공사업 ") || colums[1].contains("배관 ") || colums[1].contains("건설장비")) {
            ar.add("42");
         }
         if (colums[1].contains("자동차 판매")) {
            ar.add("45");
         }
         if (colums[1].contains("도매") || colums[1].contains("중개") || colums[1].contains("도소매")) {
            ar.add("46");
         }
         if (colums[1].contains("소매") || colums[1].contains("면세점")) {
            ar.add("47");
         }
         if (colums[1].contains("조종") || colums[1].contains("화물운송")) {
            ar.add("49");
         }
         if (colums[1].contains("해상운송")) {
            ar.add("50");
         }
         if (colums[1].contains("항공운송")) {
            ar.add("51");
         }
         if (colums[1].contains("창고운영") || colums[1].contains("스튜어") || colums[1].contains("화물운송")) {
            ar.add("52");
         }
         if (colums[1].contains("숙박") || colums[1].contains("호텔리어")) {
            ar.add("55");
         }
         if (colums[1].contains("요리") || colums[1].contains("조리학") || colums[1].contains("식품영양")) {
            ar.add("56");
         }
         if (colums[1].contains("게임") || (colums[1].contains("컴퓨터공학") && colums[1].contains("소프트웨어"))
               || colums[1].contains("인쇄") || colums[1].contains("시스템 프로그래밍")) {
            ar.add("58");
         }
         if (colums[1].contains("배급사") || colums[1].contains("방송") || colums[1].contains(" 미디어")
               || colums[1].contains("음악학")) {
            ar.add("59");
         }
         if (colums[1].contains("방송업") && colums[1].contains("방송국")) {
            ar.add("60");
         }
         if (colums[1].contains("통신공학")) {
            ar.add("61");
         }
         if (colums[1].contains("컴퓨터공학과") || colums[1].contains("소프트웨어공학") || colums[1].contains("컴퓨터과")
               || colums[1].contains("IT학부") || colums[1].contains("소프트웨어 엔지니어")
               || colums[1].contains("시스템 프로그래밍")) {
            ar.add("62");
         }
         if (colums[1].contains("데이터 베이스") || colums[1].contains("데이터베이스")) {
            ar.add("63");
         }
         if (colums[1].contains("금융") || colums[1].contains("은행")) {
            ar.add("64");
         }
         if (colums[1].contains("보험")) {
            ar.add("65");
         }
         if (colums[1].contains("부동산")) {
            ar.add("68");
         }
         if (colums[1].contains("연구원") || colums[1].contains("약학")) {
            ar.add("70");
         }
      if(colums[1].contains("보석"))
      {
        ar.add("23");
      }
      if(colums[1].contains("금속공학") || colums[1].contains("금속소재"))
      {
        ar.add("06");
      }
         if (colums[1].contains("변호사") || colums[1].contains("변리사") || colums[1].contains("회계사")
               || colums[1].contains("세무사") || colums[1].contains("광고업") || colums[1].contains("컨설팅") || colums[1].contains("전문 서비스") ||colums[1].contains("경영학")) {
            ar.add("71");
         }
         if (colums[1].contains("건축학") || colums[1].contains("조경") || colums[1].contains("도시계획")
               || colums[1].contains("측량") || (colums[1].contains("엔지니어") && colums[1].contains("서비스"))
               || colums[1].contains("A/S") || colums[1].contains("공업디자인")
               || (colums[1].contains("공업") && colums[1].contains("디자인"))) {
            ar.add("72");
         }
         if (colums[1].contains("수의학") || colums[1].contains("디자인과") || colums[1].contains("번역")
               || colums[1].contains("사진학") || colums[1].contains("서비스업") || colums[1].contains("사진 전문") || colums[1].contains("패션")) {
            if (ar.contains("71"))
               ;
            else
               ar.add("73");
         }
         if (colums[1].contains("조경관리") || colums[1].contains("청소") || (colums[1].contains("소독") && colums[1].contains("서비스") ) ) {
            ar.add("74");
         }
         if (colums[1].contains("알선") || colums[1].contains("여행사") || colums[1].contains("고용지원")
               || colums[1].contains("경호") || colums[1].contains("사무직") || colums[1].contains("사업지원")) {
            ar.add("75");

         }
         if (colums[1].contains("임대")) {
            ar.add("76");
         }
         if (colums[1].contains("행정") || colums[1].contains("복지") || colums[1].contains("입법") || colums[1].contains("공무원")|| colums[1].contains("경찰") || colums[1].contains("검찰") || colums[1].contains("소방") || colums[1].contains(" 국방")) {
            ar.add("84");
         }
         if (colums[1].contains("교사") || colums[1].contains("교수") || colums[1].contains("학원") ||  colums[1].contains("선생님")) {
            ar.add("85");
         }
         if (colums[1].contains("보건") || colums[1].contains("병원") || colums[1].contains("의예")) {
            ar.add("86");
         }

         if (colums[1].contains("복지시설") || colums[1].contains("보육시설") || colums[1].contains("복지관") || colums[1].contains("상담 서비스")) {
            ar.add("87");
         }
      if(colums[1].contains("재료공학"))
        ar.add("07");
         if (colums[1].contains("예술가") || colums[1].contains("무용") || colums[1].contains("연극")
               || colums[1].contains("뮤지컬") || colums[1].contains("도서관") || colums[1].contains("사서")
               || colums[1].contains("여가사업") || colums[1].contains("여가") || colums[1].contains("예술문화")) {
            ar.add("90");
         }
         if (colums[1].contains("스포츠") || colums[1].contains("오락") || colums[1].contains("바둑")) {
            ar.add("91");
         }
         if (colums[1].contains("종교") || colums[1].contains("전문가") || colums[1].contains("노동")
               || colums[1].contains("협회")) {
            ar.add("94");
         }
         if (colums[1].contains("수리업") || colums[1].contains("A/S") || colums[1].contains("기계공")
               || colums[1].contains("통신장비")) {
            ar.add("95");
         }
         if (colums[1].contains("미용") || colums[1].contains("마사지") || colums[1].contains("장례식")
               || colums[1].contains("간병")) {
            ar.add("96");
         }
         if (colums[1].contains(" ")) {
            if (ar.isEmpty()) {
               ar.add(0, "98");
            }
         }
      if(colums[1].contains("금융학"))
      {
        ar.add("66");
      }
         if (colums[1].contains("외교") || colums[1].contains("국제")) {
            ar.add("99");
         }
      if(colums[1].contains("연구개발"))
        ar.add("70");
         if (major.contains("금속")) {
            if (ar.contains("C24") || ar.contains("C25"))
               ;
            else
               ar.add("24");
            ar.add("25");

         }
         if (major.contains("과학")) {
            if (ar.contains("70"))
               ;
            else
               ar.add("70");
         }
      

         
         if (major.contains("가스냉동과")) {
            ar.add("41");
         }
      
      if(major.contains("CAD"))
      {
        ar.add("26");
        ar.add("27");
        ar.add("28");
        ar.add("29");
      }
         // detail = colums[1];

         select = ar.toString();
         
      } catch (Exception e) {
         System.out.println("Error parsing a record :" + e.getMessage());
      }
   }

   public String getmajor() {
      return major;
   }

   public String getcode() {
      return select;
   }


}