# NGL FAF5 물류 허브 입지 선정

NGL Transportation의 미국 내 물류 허브(Warehouse) 신규 입지 선정을 지원하기
위한 데이터셋 구축 프로젝트. FAF5 트럭 화물 통계에 도착지 수요 지표
(Pull Factors)와 출발지 항구 물동량(Push Factors)을 결합하여 주(State) 단위
Origin-Destination 데이터셋을 생성한다.

## 실행 방법

```bash
uv sync
uv run python main.py download    # 원천 데이터 다운로드 (약 150MB, FAF5)
uv run python main.py preprocess  # data/processed/faf5_hub_dataset.parquet 생성
uv run python main.py all         # 다운로드 + 전처리 일괄 실행
```

## 결합 데이터 소스

| 구분 | 출처 | 파일 | 비고 |
|---|---|---|---|
| 화물 통계 | FAF5.7.1 State (ORNL/BTS) | `data/raw/faf5/FAF5.7.1_State.csv` | 2017-2024 실측 + 2030/35/40/45/50 예측. 본 파이프라인은 실측만 사용 |
| 주별 GDP | FRED (연간, 명목) | `data/raw/fred/gdp_{ABBR}.csv` | 시리즈 `{ABBR}NGSP`, 단위 백만 USD |
| 주별 실업률 | FRED (월별) | `data/raw/fred/unemp_{ABBR}.csv` | 시리즈 `{ABBR}UR`, 연 평균으로 변환 |
| 권역별 CPI | FRED (월별) | `data/raw/fred/cpi_{REGION}.csv` | 북동/중서/남부/서부 4개 권역 (주별 CPI는 미발행) |
| 주별 인구 | US Census Bureau | `NST-EST2024-ALLDATA.csv` + `nst-est2020-alldata.csv` | 2010-2020 / 2020-2024 빈티지 결합 |
| 항구 컨테이너 TEU | BTS / AAPA 공개 통계 | `data/raw/ports/us_port_teu.csv` | 미국 주요 14개 항구 큐레이션 |

## 전처리 파이프라인 (기획서 4단계)

- **Step 1 — FAF5 기본 전처리**: `dms_mode == 1` (Truck)만 추출,
  품목(SCTG)·교역 유형(trade_type)·거리대(dist_band) 차원을 합산하여
  (출발 주, 도착 주, 연도) 단위로 집계
- **Step 2 — Pull Factors 결합**: 도착 주(`destination_state`) + 연도 기준으로
  GDP, 실업률, CPI, 인구를 LEFT JOIN
- **Step 3 — Push Factors 결합**: 출발 주(`origin_state`) + 연도 기준으로
  항구 TEU 총합·항구 수를 LEFT JOIN (항구 없는 주는 0으로 채움)
- **Step 4 — 정제 및 검증**: 결측치율 출력, 유효 주 코드 외 데이터 제외,
  Parquet 및 프리뷰 CSV 저장

## 산출 데이터셋 스키마

`data/processed/faf5_hub_dataset.parquet` — 행 = (연도, 출발 주, 도착 주)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `year` | int | 2017-2024 |
| `origin_state`, `destination_state` | str | 2글자 주 약자 |
| `origin_state_name`, `destination_state_name` | str | 주 정식 명칭 |
| `tons` | float | **타겟 변수.** FAF5 단위(천 톤) 기준 연간 트럭 화물량 |
| `value_musd` | float | 트럭 화물 가치 (기준연도 백만 USD) |
| `ton_miles` | float | 톤-마일 |
| `dest_gdp_musd` | float | 도착 주 명목 GDP (백만 USD) |
| `dest_unemployment_rate` | float | 도착 주 실업률 (연 평균) |
| `dest_cpi` | float | 도착 주 소속 권역 CPI (연 평균) |
| `dest_population` | int | 도착 주 인구 추정치 |
| `origin_port_teu` | int | 출발 주 항구의 연간 TEU 합계 (항구 없는 주는 0) |
| `origin_port_count` | int | 출발 주의 주요 항구 수 |
| `origin_has_port` | 0/1 | 출발 주에 항구 존재 여부 |
| `is_intrastate` | 0/1 | 출발 주 == 도착 주 여부 |
| `origin_fips`, `destination_fips` | int | FIPS 코드 |

현재 출력 기준: **19,944행 × 18컬럼**, 51개 주(50개 + DC), 결측치 0%.

## 프로젝트 구조

```
data/             # 전체 gitignore 처리 — 파이프라인으로 재생성
  raw/          # 다운로드한 원천 데이터
  interim/      # 중간 산출물
  processed/    # 모델링용 최종 데이터셋 (parquet + 프리뷰 CSV)
src/
  data/           # 데이터 수집/전처리 모듈
    reference.py    # 주/FIPS/권역 매핑 테이블
    download.py     # FAF5, FRED, Census, 항구 TEU 다운로더
    preprocess.py   # 기획서 4단계 결합 파이프라인
  models/         # 모델 학습/평가/추론 코드
    train.py        # 학습 루프, 모델 직렬화
    evaluate.py     # 검증·테스트 지표 산출 (RMSE, MAPE 등)
    predict.py      # 학습된 모델 로드 + 신규 입력 예측
  configs/        # 실험 설정 (하이퍼파라미터, 피처 목록, 데이터 분할)
    baseline.yaml   # 기준 모델 설정 — 같은 코드를 여러 설정으로 재사용
  utils/
notebooks/        # 분석/EDA 노트북 보관
main.py           # CLI 진입점 (download / preprocess / all)
```

## 데이터 처리 시 유의 사항

### 1. CPI(소비자물가지수)는 "주별 데이터"가 존재하지 않는다

기획서에는 "주별 CPI"라고 적혀 있지만, 사실 미국 노동통계국(BLS)은 주 단위로
CPI를 발표하지 않는다. 대신 미국 전체를 4개 권역(북동부 / 중서부 / 남부 / 서부)
으로 묶어서 발표한다. 그래서 본 프로젝트에서는 **각 주가 속한 권역의 CPI 값을
그 주의 CPI 값으로 대신 사용**한다.

예: 캘리포니아·워싱턴·오리건은 모두 서부(West) 권역이므로 같은 CPI 값을
공유한다. 모델은 "지역 단위 물가 흐름"으로 이 변수를 해석하면 된다. 권역 매핑은
`src/data/reference.py`의 `ABBR_TO_REGION`에 정의되어 있다.

### 2. FAF5 화물량의 단위는 "천 톤"이다

FAF5 데이터의 `tons_2017` 같은 컬럼은 숫자가 "톤"이 아니라 **"천 톤(thousands
of tons)"** 단위로 기록되어 있다. 즉 `tons = 100`이면 실제로는 10만 톤의 화물이
운송되었다는 의미다. 모델 결과를 해석하거나 비즈니스에 보고할 때 단위를 잘못
읽지 않도록 주의해야 한다.

### 3. 항구 TEU 데이터는 직접 큐레이션한 것이다

FRED나 Census처럼 한 줄 코드로 받을 수 있는 공식 API가 없기 때문에, BTS와
AAPA가 발표한 공개 통계 자료를 보고 **미국 주요 14개 항구의 연도별 TEU 값을
사람이 직접 코드에 입력해 두었다**. 이 데이터는 [src/data/download.py](src/data/download.py)
파일의 `PORT_TEU_ROWS` 변수에 들어 있다. 새로운 항구를 추가하거나 최신 연도
값을 갱신하려면 이 변수에 행을 추가하면 된다.

TEU는 "Twenty-foot Equivalent Unit"의 약자로, 길이 20피트짜리 컨테이너 1개를
1 TEU로 환산하는 컨테이너 화물 표준 단위다.

### 4. 인구 데이터는 두 개의 파일을 이어 붙여서 만든다

미국 Census Bureau는 10년 단위로 한 번씩 인구 집계 파일의 "버전(빈티지)"을
새로 발표한다. 우리에게 필요한 2017-2024년 구간은 어느 한 파일에도 통째로
들어있지 않다.

- 2010-2020 빈티지 파일 → 2017, 2018, 2019년 인구를 가져옴
- 2020-2024 빈티지 파일 → 2020, 2021, 2022, 2023, 2024년 인구를 가져옴
- 두 파일이 모두 가지고 있는 2020년은 **더 최신(2020-2024) 파일의 값을 사용**한다.
  최신 빈티지가 그동안의 보정값을 반영한 더 정확한 추정치이기 때문.

### 5. FAF5에는 화물 운송 수단이 8가지 있는데, 그중 트럭만 사용한다

FAF5 데이터의 `dms_mode` 컬럼은 운송 수단을 숫자 코드로 표시한다.

| 코드 | 운송 수단 |
|---|---|
| **1** | **트럭 (← 본 프로젝트에서 사용)** |
| 2 | 철도 |
| 3 | 수운 |
| 4 | 항공 |
| 5 | 복합 운송 및 우편 |
| 6 | 파이프라인 |
| 7 | 기타 / 미상 |
| 8 | 국내 운송 없음 |

NGL은 트럭 기반 First-Mile/Drayage 사업자이므로 코드 1(Truck)만 필터링하고
나머지 운송 수단은 분석 대상에서 제외한다. 이 필터 한 줄로 원본 약 120만 행이
약 40만 행으로 줄어든다.
