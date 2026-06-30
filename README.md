# ITIS_Collector_MQTT

ITIS_Collector_MQTT는 반도체 소성로 장비의 센서 데이터를 ITIS Capture를 통해 수집하고, MQTT 브로커를 거쳐 데이터베이스에 적재하는 견고한 데이터 파이프라인 시스템입니다. 파일 기반의 영속적 큐(Persistent Queue)를 사용하여 데이터의 무결성을 보장합니다.

## 시스템 아키텍처 흐름  
데이터는 다음의 파이프라인을 따라 흐릅니다:
1. **데이터 수집**: MQTT 브로커를 통해 센서 데이터를 수집합니다(`mqtt_collector_py`).  
2. **데이터 매핑**: `data_mapper.py`와 `mapping.json`을 통해 원본 데이터를 DB 레코드 형태로 변환합니다.  
3. **영속적 큐(PQ)관리**: `file_queue.py`를 통해 데이터를 로컬 큐에 저장합니다. Commit/Rollback 메커니즘을 지원합니다.  
4. **데이터 로드**: `loader.py`를 통해 DB에 데이터를 적재(INSERT/SELECT)합니다.  
5. **로깅**: `logger.py`를 통해 시스템 상태를 실시간 기록합니다.  

## 프로젝트 구조
```text
ITIS_Collector_MQTT/
├── main.py              # 메인 루프 및 파이프라인 제어
├── mqtt_collector.py    # MQTT 데이터 수집
├── data_mapper.py       # 데이터 변환 로직
├── file_queue.py        # 영속적 큐 관리
├── loader.py            # DB 적재 모듈
├── logger.py            # 로그 시스템
├── mapping.json         # 매핑 규칙 정의
├── .env.example         # 환경 설정 템플릿
├── requirements.txt     # 의존성 패키지 목록
└── tests                # pytest 테스트를 위한 폴더
```

## 환경 설정(.env)  
모든 주요 설정은 `.env` 파일을 통해 관리됩니다. 주요 항목은 다음과 같습니다:  
* **MQTT**: 서버 주소, 포트, 토픽 정보  
* **PG(Persistent Queue)**: 큐 파일 경로, 최대 파일 사이즈, 제한 여부.  
* **DB**: 데이터베이스 타입(MsSQL, Oracle, PostgreSQL 등)및 서버 접속 정보.  
* **Loader**: 데이터 적재 시 재시도 횟수 및 배치 사이즈.  
* **로그**: 로그 레벨, 파일 크기, 보관 개수 제한 및 저장 경로.  
* **Mapper**: 페이로드 매핑 규칙.  

## 설치 및 시작하기
### 1. 환경 설정  
프로젝트를 복제하고 필요한 라이브러리를 설치합니다.  
``` text  
git clone [https://github.com/Jian-02/ITIS_Collector_MQTT.git](https://github.com/Jian-02/ITIS_Collector_MQTT.git)
cd ITIS_Collector_MQTT
pip install -r requirements.txt
```  
### 2. 설정  
`.env.example` 파일을 복사하여 `.env` 파일을 생성하고, 사용하는 MQTT 브로커 정보를 입력하세요.  
```text  
cp .env.example .env
```  
### 3. 실행  
모든 설정이 완료되었다면 아래 명령어로 수집기를 시작합니다.  
```text
python main.py
```  

## 라이선스  
본 프로젝트는 MIT License 를 따릅니다.  