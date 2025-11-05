# 📊 교육 설문 플랫폼

Streamlit 기반 교육 과정 설문 관리 및 분석 시스템입니다. 구성원들의 설문 참여와 관리자의 실시간 분석을 지원합니다.

## 🚀 빠른 시작

### 1. 필요한 라이브러리 설치
```bash
pip install -r requirements.txt
```

### 2. Google Service Account 설정
- `huhsame-service-account-key.json` 파일이 프로젝트 루트에 있어야 합니다
- Google Sheets에 서비스 계정 이메일을 공유(편집 권한) 해야 합니다

### 3. 환경 설정 (선택사항)

#### 방법 A: 환경변수 설정 (PowerShell)
```powershell
setx GOOGLE_SHEETS_SPREADSHEET_ID "1sxwBgqSqxHw1mqfxAHskspO-SCpEDWTAioII_pp7hHs"
setx SURVEY_ADMIN_PASSWORD "원하는_비밀번호"
```

#### 방법 B: Streamlit Secrets 설정
`.streamlit/secrets.toml` 파일 생성:
```toml
GOOGLE_SHEETS_SPREADSHEET_ID = "1sxwBgqSqxHw1mqfxAHskspO-SCpEDWTAioII_pp7hHs"
SURVEY_ADMIN_PASSWORD = "원하는_비밀번호"
```

### 4. 앱 실행
```bash
streamlit run survey_app.py
```

## 🔐 관리자 인증 정보

### 기본 비밀번호
- **관리자 비밀번호**: `skms2024`

### 비밀번호 변경 방법
1. 환경변수 또는 `.streamlit/secrets.toml`에 `SURVEY_ADMIN_PASSWORD` 설정
2. 또는 `survey_app.py`의 `get_admin_password()` 함수에서 fallback 값 변경

## 📋 주요 기능

### 👥 일반 사용자 모드
- **설문 참여**: 활성화된 교육 과정의 설문에 응답
- **다양한 문항 유형 지원**: 
  - 객관식 (단일/다중 선택)
  - 주관식 (단답/장문)
  - 평점형 (1-10점 척도)
- **필수 문항 검증**: 필수 항목 미응답 시 제출 방지
- **실시간 저장**: Google Sheets에 즉시 반영

### 🔐 관리자 모드
#### Phase 1: 설문 관리 시스템 ✅
- **과정 관리**: 교육 과정 생성/수정/삭제
- **설문 편집기**: 
  - 객관식/주관식/평점형 문항 추가
  - 문항 순서 조정
  - 필수 여부, 글자 수 제한 설정
- **활성화 토글**: 설문 공개/비공개 전환
- **미리보기**: 설문 응답 화면 미리 확인
- **DB 스키마 관리**: Google Sheets 시트 자동 생성 및 초기화

#### Phase 2: 응답 수집 시스템 ✅
- **실시간 데이터 수집**: 응답 즉시 Google Sheets 저장
- **응답 통계**: 응답률, 응답자 수 자동 집계
- **개인정보 보호**: 
  - 응답자 ID 해시화
  - IP 마스킹 처리
  - 세션 관리

#### Phase 3: 분석 보고서 시스템 ✅
- **데이터 시각화**:
  - 객관식: 막대 차트로 선택지별 분포 표시
  - 평점형: 원형 차트로 평점 분포 및 평균 점수 표시
  - 주관식: 워드 클라우드 및 전체 응답 목록
- **KPI 대시보드**: 총 응답자 수, 문항 수, 응답률 실시간 표시
- **Gemini AI 인사이트**: 설문 결과를 AI가 분석하여 주요 인사이트와 개선 제안 제공
- **분석 결과 저장**: AI 분석 결과를 Analysis 시트에 자동 저장

## 📊 데이터 구조

### Google Sheets 시트 구성
1. **Courses**: 과정 정보
   - courseId, title, description, category, createdAt, status, ownerId

2. **SurveySettings**: 설문 설정
   - courseId, isActive, startDate, endDate, maxResponses

3. **Questions**: 설문 문항
   - questionId, courseId, order, text, type, choicesJson, ratingMax, isRequired, maxChars

4. **Responses**: 설문 응답
   - responseId, courseId, questionId, answer, answeredAt, respondentHash, sessionId, ipMasked

5. **ResponseStats**: 응답 통계
   - courseId, totalQuestions, totalResponses, responseRate, lastUpdatedAt

6. **Analysis**: AI 분석 결과 (예정)
   - analysisId, courseId, analyzedAt, objectiveJson, ratingJson, subjectiveJson, insightsText, actionItemsText, confidence

## 🎯 사용 시나리오

### 시나리오 1: 관리자가 새로운 설문 생성
1. 사이드바에서 "관리자" 모드 선택
2. 비밀번호 입력하여 인증 (`skms2024`)
3. "과정 리스트" 탭에서 "새 과정 만들기" 클릭
4. 과정 정보 입력 후 저장
5. "설문 편집" 클릭하여 문항 추가
6. 문항 추가 후 "설문 활성화" 토글 ON

### 시나리오 2: 구성원이 설문 참여
1. 사이드바에서 "일반 사용자" 모드 선택 (기본값)
2. 참여 가능한 설문 목록에서 선택
3. 각 문항에 응답
4. "설문 제출" 버튼 클릭
5. 성공 메시지 및 풍선 애니메이션 확인

### 시나리오 3: 관리자가 응답 확인 (Phase 3 예정)
1. 관리자 모드로 인증
2. "결과 보기" 클릭
3. 대시보드에서 실시간 통계 확인
4. AI 인사이트 및 개선 제안 검토
5. 필요시 보고서 다운로드

## 🛠️ 기술 스택

- **프론트엔드**: Streamlit
- **데이터베이스**: Google Sheets (gspread API)
- **AI/분석**: Google Gemini AI (예정)
- **시각화**: Plotly, WordCloud (예정)
- **보안**: hashlib, cryptography (Fernet)

## 🔧 문제 해결

### Google Sheets API 오류
- 서비스 계정이 시트에 공유되었는지 확인
- `huhsame-service-account-key.json` 파일 경로 확인
- Google Sheets API가 활성화되었는지 확인

### 관리자 인증 실패
- 비밀번호가 `skms2024`인지 확인
- 환경변수가 설정된 경우 해당 값 확인
- 세션이 만료되었을 수 있음 (30분 타임아웃)

### 설문이 보이지 않음
- 관리자가 설문을 활성화했는지 확인
- "DB 설정" 탭에서 "스키마 보증 실행" 클릭
- 페이지 새로고침

## 📁 파일 구조

```
aicoding/
├── survey_app.py              # 메인 Streamlit 앱
├── gsheets_utils.py           # Google Sheets 유틸리티
├── requirements.txt           # Python 패키지 목록
├── huhsame-service-account-key.json  # 서비스 계정 키
├── .streamlit/
│   └── secrets.toml          # Streamlit secrets (선택)
└── SURVEY_README.md          # 이 파일
```

## 🚧 개발 로드맵

### ✅ Phase 1: 설문 관리 시스템 (완료)
- 과정 CRUD
- 설문 편집기
- 활성화 토글
- 미리보기

### ✅ Phase 2: 응답 수집 시스템 (완료)
- 설문 참여 폼
- 실시간 데이터 저장
- 응답 통계 업데이트
- 개인정보 보호

### ✅ Phase 3: 분석 보고서 시스템 (완료)
- 데이터 시각화 (막대/원형 차트, 워드클라우드)
- Gemini AI 인사이트 생성
- 관리자 대시보드 (4개 탭: 객관식/평점/주관식/AI 인사이트)

### 📅 Phase 4: 고도화 (향후)
- 개인별 학습 이력 추적
- 맞춤형 과정 추천
- 웹페이지 임베드 (iframe/위젯)
- REST API 제공

## 💡 팁

1. **설문 미리보기**: 설문을 활성화하기 전에 미리보기로 확인하세요
2. **필수 문항 설정**: 중요한 질문은 필수 문항으로 설정하세요
3. **글자 수 제한**: 주관식 문항에 적절한 최대 글자 수를 설정하세요
4. **정기적인 백업**: Google Sheets를 정기적으로 복사하여 백업하세요

## 📞 지원

문제가 발생하거나 기능 요청이 있으시면 프로젝트 관리자에게 문의하세요.

---

**Version**: 2.0.0 (Phase 1-3 완료)  
**Last Updated**: 2024년 10월

## 🎉 Phase 3 추가 기능 사용법

### 📊 분석 대시보드 접근
1. 관리자 모드로 로그인
2. "과정 리스트"에서 분석하고자 하는 과정의 "결과 보기" 클릭
3. 대시보드에서 4개 탭 확인:
   - **객관식**: 선택지별 응답 분포 막대 차트
   - **평점**: 평점 분포 원형 차트 및 평균 점수
   - **주관식**: 워드 클라우드 (응답 5개 이상 시) 및 전체 응답 목록
   - **AI 인사이트**: "AI 분석 실행" 버튼 클릭 시 Gemini가 인사이트 생성

### 🤖 Gemini AI 설정
- 환경변수 또는 `.streamlit/secrets.toml`에 설정:
```toml
GEMINI_API_KEY = "your_gemini_api_key_here"
```
- API 키 발급: https://ai.google.dev/

### 📈 워드 클라우드 생성 조건
- 주관식 문항에 최소 5개 이상의 응답이 있어야 워드 클라우드가 생성됩니다
- 5개 미만인 경우 전체 응답 목록만 표시됩니다

