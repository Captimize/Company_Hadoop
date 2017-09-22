package com.company;

import java.util.HashMap;

import org.apache.hadoop.io.Text;

public class AbroadInfoParser {
	private String company_country;
	private String company_name;
	private String code;
	private String prevalue;
	private String owner;
	private String company_number;
	private String address;
	private String site;

	HashMap<String, String> categorying = new HashMap();

  public AbroadInfoParser(Text text) {
    try {
      String[] colums = text.toString().split("#");

          if (colums != null && colums.length > 0) {
        	  if (!colums[0].contains("상관")){
    					company_country = colums[0].trim();
    					company_name = colums[1].trim();
    					owner = colums[2].trim();
    					company_number = colums[3].trim();
    					address = colums[5].trim();
    					site = colums[7].trim();

    					if (owner.isEmpty())
    						owner = "-";
    					if (company_number.isEmpty())
    						company_number = "-";
    					if (address.isEmpty())
    						address = "-";
    					if (site.isEmpty())
    						site = "-";

    					MappingCode();

    					code = categorying.get(colums[4]);

    					prevalue = colums[4];
    				 }
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public String getprevalue() {
		return prevalue;
	}

	public String getcompanycountry() {
		return company_country;
	}

	public String getcode() {
		return code;
	}

	public String getcompanyname() {
		return company_name;
	}

	public String getOwner() {
		return owner;
	}

	public String getAddress() {
		return address;
	}

	public String getCompNum() {
		return company_number;
	}

	public String getSite() {
		return site;
	}
  public void MappingCode() {

		categorying.put("제조업", "00");
		categorying.put("농업", "01");
		categorying.put("임업", "02");
		categorying.put("어업", "03");
		categorying.put("광업", "05");
		categorying.put("석탄, 원유 및 천연가스 광업", "05");
		categorying.put("금속 광업", "06");
		categorying.put("비금속광물 광업", "07");
		categorying.put("광업 지원 서비스업", "08");

		categorying.put("식료품 제조업", "10");
		categorying.put("음료 제조업", "11");
		categorying.put("담배 제조업", "12");
		categorying.put("섬유제품 제조업", "13");
		categorying.put("섬유제품 제조업; 의복제외", "13");
		categorying.put("의복, 의복액세서리 및 모피제품 제조업", "14");
		categorying.put("가죽, 가방 및 신발 제조업", "15");
		categorying.put("목재 및 나무제품 제조업", "16");
		categorying.put("펄프, 종이 및 종이제품 제조업", "17");

		categorying.put("인쇄 및 기록매체 복제업", "18");
		categorying.put("코크스, 연탄 및 석유정제품 제조업", "19");
		categorying.put("화학물질 및 화학제품 제조업", "20");
		categorying.put("화학물질 및 화학제품 제조업;의약품 제외", "20");
		categorying.put("의료용 물질 및 의약품 제조업", "21");
		categorying.put("고무제품 및 플라스틱제품 제조업", "22");
		categorying.put("비금속 광물제품 제조업", "23");
		categorying.put("1차 금속 제조업", "24");
		categorying.put("금속가공제품 제조업", "25");
		categorying.put("금속가공제품 제조업;기계 및 가구 제외", "25");

		categorying.put("전자부품, 컴퓨터, 영상, 음향 및 통신장비 제조업", "26");
		categorying.put("컴퓨터 및 주변장치 제조업", "26");
		categorying.put("의료, 정밀, 광학기기 및 시계 제조업", "27");
		categorying.put("전기장비 제조업", "28");
		categorying.put("기타 기계 및 장비 제조업", "29");
		categorying.put("자동차 및 트레일러 제조업", "30");
		categorying.put("기타 운송장비 제조업", "31");
		categorying.put("그외 기타 운송장비 제조업", "31");
		categorying.put("가구 제조업", "32");
		categorying.put("기타 제품 제조업", "33");

		categorying.put("전기, 가스, 증기 및 공기조절 공급업", "35");
		categorying.put("수도사업", "36");
		categorying.put("하수, 폐수 및 분뇨 처리업", "37");
		categorying.put("하수 · 폐기물 처리, 원료재생 및 환경복원업", "37");
		categorying.put("폐기물 수집운반, 처리 및 원료재생업", "38");
		categorying.put("환경 정화 및 복원업", "39");
		categorying.put("토양 및 지하수 정화업", "39");
		categorying.put("종합 건설업", "41");
		categorying.put("건설업", "41");
		categorying.put("사무 및 상업용 건물 건설업", "41");
		categorying.put("전문직별 공사업", "42");
		categorying.put("일반 통신 공사업", "42");
		categorying.put("자동차 및 부품 판매업", "45");
		categorying.put("자동차 타이어 및 튜브 판매업", "45");
		categorying.put("자동차 판매업", "45");
		categorying.put("도매 및 상품중개업", "46");
		categorying.put("상품 중개업", "46");
		categorying.put("도매 및 소매업", "46");

		categorying.put("소매업", "47");
		categorying.put("통신기기 소매업", "47");

		categorying.put("소매업; 자동차 제외", "47");

		categorying.put("육상운송 및 파이프라인 운송업", "49");
		categorying.put("수상 운송업", "50");
		categorying.put("항공 운송업", "51");
		categorying.put("운수업", "52");
		categorying.put("창고 및 운송관련 서비스업", "52");
		categorying.put("숙박업", "55");
		categorying.put("음식점 및 주점업", "56");
		categorying.put("일식 음식점업", "56");
		categorying.put("숙박 및 음식점업", "56");
		categorying.put("한식 음식점업", "56");
		categorying.put("출판업", "58");
		categorying.put("잡지 및 정기간행물 발행업", "58");

		categorying.put("영상·오디오 기록물 제작 및 배급업", "59");
		categorying.put("방송업", "60");
		categorying.put("통신업", "61");
		categorying.put("전기통신업", "61");
		categorying.put("컴퓨터 프로그래밍, 시스템 통합 및 관리업", "62");
		categorying.put("정보서비스업", "63");
		categorying.put("기타 정보 서비스업", "63");
		categorying.put("금융업", "64");
		categorying.put("기타 투자기관", "64");
		categorying.put("부동산업 및 임대업", "64");
		categorying.put("보험 및 연금업", "65");
		categorying.put("금융 및 보험 관련 서비스업", "66");

		categorying.put("부동산업", "68");
		categorying.put("임대업", "69");
		categorying.put("연구개발업", "70");
		categorying.put("전문서비스업", "71");
		categorying.put("회계 및 세무관련 서비스업", "71");
		categorying.put("광고업", "71");
		categorying.put("건축기술, 엔지니어링 및 기타 과학기술 서비스업", "72");
		categorying.put("기타 전문, 과학 및 기술 서비스업", "73");
		categorying.put("전문, 과학 및 기술 서비스업", "73");
		categorying.put("그외 기타 분류안된 전문, 과학 및 기술 서비스업", "73");
		categorying.put("사업시설 관리 및 조경 서비스업", "74");
		categorying.put("사업지원 서비스업", "75");

		categorying.put("임대업", "84");
		categorying.put("외무 행정", "84");
		categorying.put("공공행정, 국방 및 사회보장 행정", "85");
		categorying.put("교육지원 서비스업", "85");
		categorying.put("기타 교육지원 서비스업", "85");
		categorying.put("교육 서비스업", "86");
		categorying.put("보건업 및 사회복지 서비스업", "86");
		categorying.put("보건업", "87");
		categorying.put("사회복지 서비스업", "88");
		categorying.put("창작, 예술 및 여가관련 서비스업", "90");
		categorying.put("스포츠 및 오락관련 서비스업", "91");
		categorying.put("유원지 및 기타 오락관련 서비스업", "91");

		categorying.put("협회 및 단체", "94");
		categorying.put("수리업", "95");
		categorying.put("자동차 종합 수리업", "95");
		categorying.put("기타 개인 서비스업", "96");
		categorying.put("가구내 고용활동", "97");
		categorying.put("달리 분류되지 않은 자가소비를 위한 가구의 재화 및 서비스 생산활동", "98");
		categorying.put("국제 및 외국기관", "99");

	}

  
}
