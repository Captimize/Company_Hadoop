package com.company;

import org.apache.hadoop.io.Text;

public class FundingInfoParser {
	private String funding_company;
	private String category;
  private String detail;
  private String period;
  private String funding_cost;
  private String code;
	private String ar = null;
  //private boolean departureDelayAvailable = true;
  //private boolean distanceAvailable = true;

  //private String uniqueCarrier;

  public FundingInfoParser(Text text) {
    try {
      String[] colums = text.toString().split("#");
      if (colums != null && colums.length > 0) {
    	  funding_company = colums[0];
    	  category = colums[1];
    	  if(colums[2].contains(funding_company)){
    		  detail = "-";
    	  }else{
    		  detail = colums[2];
    	  		}
    	  period = colums[3];
    	  funding_cost = colums[4].substring(0,colums[4].indexOf("원")-1);
    	  if (colums[1].contains("제조") || colums[2].contains("제조")) {
				if (colums[2].contains("자동차")) {
					ar = "30";
				} else if (colums[2].contains("보트"))
					ar = "31";
				else if (colums[2].contains("금형")) {
					ar = "29";
				} else if (colums[2].contains("도어") || colums[2].contains("테이프") || colums[2].contains("케이블")
						|| colums[2].contains("피복기") || colums[2].contains("산업용"))
					ar = "29";
				else if (colums[2].contains("가공") && colums[2].contains("조리")) {
					ar = "10";
				} else if (colums[2].contains("제과용"))
					ar = "10";

				else if (colums[2].contains("철") && colums[2].contains("가공")
						|| (colums[2].contains("금속") && colums[2].contains("가공")))
					ar = "25";
				else if (colums[2].contains("간판") || colums[2].contains("광고물")) {
					ar = "33";
				}

				else if (colums[2].contains("밸브") || colums[2].contains("파이프") || colums[2].contains("노즐"))
					ar = "29";
				else if (colums[2].contains("디바이스") && colums[1].contains("모바일")) {
					ar = "26";
				} else if (colums[2].contains("기계") || colums[2].contains("3D프린터") || colums[2].contains("산업용")) {
					ar = "29";
				} else if (colums[2].contains("플라스틱") || colums[2].contains("고무")) {
					ar = "22";

				} else if (colums[2].contains("반도체")) {
					ar = "26";
				} else if (colums[2].contains("반도체장비") || colums[2].contains("반도체 장비")) {
					ar = "29";
				} else if (colums[2].contains("장갑") || colums[2].contains("양말") || colums[2].contains("스타킹")
						|| colums[2].contains("모자") || colums[2].contains("모피")) {
					ar = "14";
				}

				else if (colums[2].contains("주철") || colums[2].contains("철강"))
					ar = "24";
				else if (colums[2].contains("에너지"))
					ar = "28";
				else if (colums[2].contains("잉크")) {
					ar = "20";
				} else if (colums[2].contains("육류")) {
					ar = "10";
				} else if (colums[2].contains("가금류")) {
					ar = "10";
				} else if (colums[2].contains("컴퓨터") || colums[2].contains("부품") || colums[2].contains("표시장치")) {
					ar = "26";
				} else if (colums[2].contains("수산물")) {
					ar = "10";
				} else if (colums[2].contains("김치")) {
					ar = "10";
				} else if (colums[2].contains("곡물")) {
					ar = "10";
				} else if (colums[2].contains("식품")) {
					ar = "10";
				} else if (colums[2].contains("사료")) {
					ar = "10";
				} else if (colums[2].contains("음료")) {
					ar = "11";
				} else if (colums[2].contains("담배")) {
					ar = "12";
				} else if (colums[2].contains("직물")) {
					ar = "13";
				} else if (colums[2].contains("섬유")) {
					ar = "13";
				} else if (colums[2].contains("원단")) {
					ar = "13";
				} else if (colums[2].contains("의류") || colums[2].contains("의복") || colums[2].contains("모피")) {
					ar = "14";
				} else if (colums[2].contains("모자")) {
					ar = "14";
				} else if (colums[2].contains("가방") || colums[2].contains("가죽") || colums[2].contains("신발")
						|| colums[2].contains("구두")) {
					ar = "15";
				} else if (colums[2].contains("목재") || colums[2].contains("나무")) {
					ar = "16";
				} else if (colums[2].contains("펄프") || colums[2].contains("종이") || colums[2].contains("원지")
						|| colums[2].contains("벽지")) {
					ar = "17";
				} else if (colums[2].contains("인쇄")) {
					ar = "18";
				} else if (colums[2].contains("연탄") || colums[2].contains("원유") || colums[2].contains("석유")) {
					ar = "19";
				} else if (colums[2].contains("화학") || colums[2].contains("가스") || colums[2].contains("합성")
						|| colums[2].contains("화장품")) {
					ar = "20";
				} else if (colums[2].contains("의료") || colums[2].contains("의약") || colums[2].contains("한의약")) {
					if (colums[2].contains("기기"))
						;
					else
						ar = "19";
				} else if (colums[2].contains("유리") || colums[2].contains("도자기") || colums[2].contains("시멘트")
						|| colums[2].contains("레미콘")) {
					ar = "23";
				} else if (colums[2].contains("제철") || colums[2].contains("철강") || colums[2].contains("제련")
						|| colums[2].contains("합금") || colums[2].contains("비금속")) {
					ar = "24";
				} else if (colums[2].contains("금속") || colums[2].contains("보일러") || colums[2].contains("방열기")
						|| colums[2].contains("무기") || colums[2].contains("총")) {
					ar = "25";
				} else if (colums[2].contains("회로") || colums[2].contains("전자") || colums[2].contains("부품")
						|| colums[2].contains("통신") || colums[2].contains("방송")) {
					ar = "26";
				}

				else if (colums[2].contains("의료기기") || colums[2].contains("안경") || colums[2].contains("광학")
						|| colums[2].contains("시계")) {
					ar = "27";
				} else if (colums[2].contains("발전기") || colums[2].contains("축전지") || colums[2].contains("조명")
						|| colums[2].contains("가전제품") || colums[2].contains("가전 제품") || colums[2].contains("가정용")) {
					ar = "28";
				} else if (colums[2].contains("운송") || colums[2].contains("선박") || colums[2].contains("철도")
						|| colums[2].contains("항공") || colums[2].contains("자전거")) {
					ar = "31";
				} else if (colums[2].contains("가구")) {
					ar = "32";
				} else if (colums[2].contains("수리")) {
					ar = "34";
				} else if (colums[2].contains("활성탄")) {
					ar = "20";
					
				}

				else
					ar = "33";

			}

			else if (colums[1].contains("유통") || colums[2].contains("유통")) {
				if (colums[2].contains("화물") || colums[2].contains("운송")) {
					ar = "49";
				} else if (colums[2].contains("무역")) {
					ar = "46";
				} else if (colums[2].contains("도매")) {
					ar = "46";
				} else if (colums[2].contains("소매")) {
					ar = "47";
				} else if (colums[2].contains("도소매") || colums[1].contains("도소매"))
					ar = "47";

				else if (colums[1].contains("모바일"))
					ar = "46";
				else
					ar = "46";
			}

			else if (colums[1].contains("정보서비스") || colums[2].contains("정보서비스")) {
				if (colums[2].contains("통신")) {
					ar = "61";
				} else if (colums[2].contains("모바일")) {
					ar = "58";
				} else if (colums[2].contains("시스템")) {
					ar = "62";
				} else if (colums[2].contains("보안")) {
					ar = "58";
				} else if (colums[2].contains("게임")) {
					ar = "58";
				} else if (colums[2].contains("제조")) {
					if (colums[2].contains("컴퓨터"))
						ar = "26";
					else if (colums[2].contains("금형"))
						ar = "29";
				} else if (colums[2].contains("컴퓨터") || colums[2].contains("프로그래밍") || colums[2].contains("소프트웨어")) {
					ar = "58";
				} else if (colums[2].contains("싸이트") || colums[2].contains("사이트"))
					ar = "62";
				else if (colums[2].contains("개발"))
					ar = "58";
				else
					ar = "63";

			}

			else if (colums[1].contains("기술서비스") || colums[2].contains("기술서비스")) {
				if (colums[2].contains("건축") || colums[2].contains("조경")) {
					ar = "72";
				} else if (colums[2].contains("건축")) {
					ar = "72";
				} else if (colums[2].contains("디자인")) {
					ar = "73";
				} else if (colums[2].contains("번역")) {
					ar = "73";
				} else if (colums[2].contains("계량")) {
					ar = "73";
				} else if (colums[2].contains("엔지니어")) {
					ar = "72";
				} else if (colums[2].contains("개발"))
					ar = "58";
				else if (colums[2].contains("컴퓨터") || colums[2].contains("프로그래밍") || colums[2].contains("소프트웨어")) {
					ar = "58";
				}

				else
					ar = "72";
			}

			else if (colums[1].contains("푸드")) {
				ar = "47";

			}

			else if (colums[1].contains("광고") || colums[1].contains("마케팅")) {
				ar = "71";
			}

			else if (colums[2].contains("건설")) {
				ar = "41";
			} else if (colums[2].contains("여행")) {
				ar = "90";
			} else if (colums[2].contains("도소매")) {
				ar = "46";
			}

			else if (colums[2].contains("반도체")) {
				if (colums[1].contains("제조") && colums[2].contains("제조")) {
					ar = "26";
				} else if (colums[1].contains("기술서비스")) {
					ar = "63";
				}

			} else if (colums[2].contains("판매") || colums[1].contains("판매")) {
				if (colums[2].contains("가전제품") || colums[2].contains("주방"))
					ar = "47";
				else if (colums[2].contains("자동차"))
					ar = "45";
				else
					ar = "47";
			} else if (colums[2].contains("플랜트") || colums[2].contains("공사 업체")) {
				ar = "41";
			} else if (colums[2].contains("수산물 도매")) {
				ar = "46";
			}

			else if (colums[2].contains("농산물도매")) {
				ar = "46";
			}

			else if (colums[2].contains("영화배급")) {
				ar = "59";
			} else if (colums[2].contains("인력")) {
				ar = "75";
			} else if (colums[2].contains("건어물 도매")) {
				ar = "46";
			}

			else if (colums[2].contains("가죽 제조") || colums[2].contains("가죽 제조")) {
				ar = "15";
			}

			else if (colums[2].contains("솔루션") && (colums[1].contains("정보서비스") || colums[1].contains("기술서비스"))) {
				ar = "58";
			}

			else if (colums[2].contains("농업") || colums[1].contains("농업")) {
				ar = "01";
			}

			else if (colums[1].contains("기타")) {
				if (colums[2].contains("자동차")) {
					ar = "30";
				}

				else if (colums[2].contains("금형")) {
					ar = "29";
				} else if (colums[2].contains("측량")) {
					ar = "72";
				} else if (colums[2].contains("기계")) {
					ar = "29";
				} else if (colums[2].contains("플라스틱") || colums[2].contains("고무")) {
					ar = "22";

				} else if (colums[2].contains("잉크")) {
					ar = "20";
				} else if (colums[2].contains("육류")) {
					ar = "10";
				} else if (colums[2].contains("가금류")) {
					ar = "10";
				} else if (colums[2].contains("수산물")) {
					ar = "10";
				} else if (colums[2].contains("김치")) {
					ar = "10";
				} else if (colums[2].contains("곡물")) {
					ar = "10";
				} else if (colums[2].contains("식품")) {
					ar = "10";
				} else if (colums[2].contains("사료")) {
					ar = "10";
				} else if (colums[2].contains("음료")) {
					ar = "11";
				} else if (colums[2].contains("담배")) {
					ar = "12";
				} else if (colums[2].contains("직물")) {
					ar = "13";
				} else if (colums[2].contains("섬유")) {
					ar = "13";
				} else if (colums[2].contains("원단")) {
					ar = "13";
				} else if (colums[2].contains("의류") || colums[2].contains("의복") || colums[2].contains("모피")) {
					ar = "14";
				} else if (colums[2].contains("모자")) {
					ar = "14";
				} else if (colums[2].contains("가방") || colums[2].contains("가죽") || colums[2].contains("신발")
						|| colums[2].contains("구두")) {
					ar = "15";
				} else if (colums[2].contains("목재") || colums[2].contains("나무")) {
					ar = "16";
				} else if (colums[2].contains("펄프") || colums[2].contains("종이") || colums[2].contains("원지")
						|| colums[2].contains("벽지")) {
					ar = "17";
				} else if (colums[2].contains("인쇄")) {
					ar = "18";
				} else if (colums[2].contains("연탄") || colums[2].contains("원유") || colums[2].contains("석유")) {
					ar = "19";
				} else if (colums[2].contains("화학") || colums[2].contains("가스") || colums[2].contains("합성")
						|| colums[2].contains("화장품")) {
					ar = "20";
				} else if (colums[2].contains("의료") || colums[2].contains("의약") || colums[2].contains("한의약")) {
					if (colums[2].contains("기기"))
						;
					else
						ar = "19";
				} else if (colums[2].contains("유리") || colums[2].contains("도자기") || colums[2].contains("시멘트")
						|| colums[2].contains("레미콘")) {
					ar = "23";
				} else if (colums[2].contains("제철") || colums[2].contains("철강") || colums[2].contains("제련")
						|| colums[2].contains("합금") || colums[2].contains("비금속")) {
					ar = "24";
				} else if (colums[2].contains("금속") || colums[2].contains("보일러") || colums[2].contains("방열기")
						|| colums[2].contains("무기") || colums[2].contains("총")) {
					ar = "25";
				} else if (colums[2].contains("회로") || colums[2].contains("전자") || colums[2].contains("부품")
						|| colums[2].contains("통신") || colums[2].contains("방송")) {
					ar = "26";
				}

				else if (colums[2].contains("의료기기") || colums[2].contains("안경") || colums[2].contains("광학")
						|| colums[2].contains("시계")) {
					ar = "27";
				} else if (colums[2].contains("발전기") || colums[2].contains("축전지") || colums[2].contains("조명")
						|| colums[2].contains("가전제품") || colums[2].contains("가전 제품") || colums[2].contains("가정용")) {
					ar = "28";
				} else if (colums[2].contains("운송") || colums[2].contains("선박") || colums[2].contains("철도")
						|| colums[2].contains("항공") || colums[2].contains("자전거")) {
					ar = "31";
				} else if (colums[2].contains("가구")) {
					ar = "32";
				} else if (colums[2].contains("수리")) {
					ar = "34";
				} else if (colums[2].contains("통신")) {
					ar = "61";
				} else if (colums[2].contains("모바일")) {
					ar = "58";
				} else if (colums[2].contains("게임")) {
					ar = "58";
				}

				else if (colums[2].contains("컴퓨터") || colums[2].contains("프로그래밍") || colums[2].contains("소프트웨어")
						|| colums[2].contains("서버")) {
					ar = "58";
				} else if (colums[2].contains("도장"))
					ar = "16";

				else if (colums[2].contains("(주)"))
					ar = "71";
				else if (colums[2].contains("엔지니어"))
					ar = "72";

				else if (colums[2].contains("합판"))
					ar = "16";
				else if (colums[2].contains("공사"))
					ar = "41";
				else if (colums[2].contains("보안") && colums[2].contains("시스템"))
					ar = "58";
				else if (colums[2].contains("판매") && colums[2].contains("가전"))
					ar = "46";
				else if (colums[2].contains("도매"))
					ar = "46";
				else if (colums[2].contains("원단"))
					ar = "13";
				else if (colums[2].contains("산업디자인"))
					ar = "73";
				else if (colums[2].contains("공급"))
					ar = "35";

			}

			else if (colums[1].contains("엔터테인먼트")) {
				ar = "73";
			}

			else if (colums[1].contains("방송통신")) {
				ar = "60";
			}

			else if (colums[1].contains("모바일")) {
				if (colums[1].contains("게임"))
					ar = "58";
			}

			else if (colums[1].contains("게임")) {
				ar = "58";
			}

			else if (colums[1].contains("헬스케어")) {
				if (colums[2].contains("기기") || colums[2].contains("기구"))
					ar = "46";
				else if (colums[2].contains("발효"))
					ar = "29";
			}

			else if (colums[1].contains("패션")) {
				if (colums[2].contains("속눈썹") || colums[2].contains("미용") || colums[2].contains("뷰티")) {
					ar = "96";
				}

				else if (colums[2].contains("도매") || colums[2].contains("도소매")) {
					ar = "46";
				} else if (colums[2].contains("원단"))
					ar = "13";

				else if (colums[2].contains("신발"))
					ar = "15";
				else if (colums[2].contains("파스너"))
					ar = "33";
				else
					ar = "73";
			}

			else if (colums[1].contains("콘텐츠")) {
				ar = "58";
			}

			else if (colums[1].contains("교육") || colums[1].contains("교육")) {
				if (colums[2].contains("출판"))
					ar = "58";

				else
					ar = "85";
			}

			else if (colums[1].contains("에너지")) {

				ar = "35";
			}

			else if (colums[2].contains("주유소"))
				ar = "47";
			else if (colums[2].contains("택배"))
				ar = "49";
			else if (colums[2].contains("하수") && colums[2].contains("처리"))
				ar = "37";
			else if (colums[2].contains("양식") && colums[2].contains("어업"))
				ar = "03";
 
			
			if(ar == null)
        		ar = "96";
			code = ar.toString();
      
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }

  public String getfundingcompany() { return funding_company; }

  public String getcategory() { return category; }
  
  public String getdetail() { return detail; }
  
  public String getperiod() { return period; }

  public String getfunding_cost() { return funding_cost; }
  
  public String getcode() { return code;}
  
}
