```python
import os
import re
import hashlib
import json
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone

import gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials


SPREADSHEET_ENV_KEY = "GOOGLE_SHEETS_SPREADSHEET_ID"
SERVICE_ACCOUNT_FILE_ENV_KEY = "GOOGLE_SERVICE_ACCOUNT_FILE"


REQUIRED_SHEETS: Dict[str, List[str]] = {
    # 1) Courses - 교육 과정 리스트
    "Courses": [
        "course_id",
        "program_name",
        "session_no",
        "theme",
        "event_type",  # NCT / Forum / Workshop / Webinar / Internal Talk
        "event_date",
        "location",
        "host_org",
        "speakers",
        "survey_form_version",
        "response_source_file",
        "status",  # planned / active / completed / archived
        "created_at",
        "updated_at",
    ],
    # 2) Survey_Items - 설문 항목 카탈로그 (표준화된 문항 관리)
    "Survey_Items": [
        "item_id",
        "item_code",  # 재사용 표준 코드 (예: SAT_OVERALL, DIFF_OVERALL, NPS)
        "item_group",  # 세션/스피치/대담 그룹명
        "item_text",  # 문항 본문
        "metric_type",  # likert / nps / single_choice / multi_choice / text
        "dimension",  # satisfaction / difficulty / understanding / insight / recommend / operations / content / nps
        "scale_min",
        "scale_max",
        "scale_label_min",
        "scale_label_max",
        "options",  # 선택지 목록 (JSON 또는 CSV)
        "applies_to_speaker",  # 스피커별 평가 문항인 경우
        "applies_to_session",  # 세션별 평가 문항인 경우
        "default_order",
        "is_active",
        "created_at",
        "updated_at",
    ],
    # 3) Course_Item_Map - 과정↔문항 매핑 (문항 재사용)
    "Course_Item_Map": [
        "map_id",
        "course_id",
        "item_id",
        "order_in_course",
        "is_required",
        "custom_item_text",  # 과정별 문항 커스터마이징 시 사용
        "created_at",
    ],
    # 4) Responses - 문항 단위 응답 (정규화)
    "Responses": [
        "response_id",
        "course_id",
        "respondent_id",
        "timestamp",
        "item_id",
        "response_value",  # 원본 텍스트
        "response_value_num",  # 정규화 수치 (리커트/NPS → 숫자)
        "choice_value",  # 다중선택 분해 시 단일 선택값
        "comment_text",  # 서술형
        "source_row_index",  # 원본 파일의 행 번호
        "ingest_batch_id",  # 적재 배치/버전
    ],
    # 5) Respondents - 응답자 정보 (PII 분리)
    "Respondents": [
        "respondent_id",
        "course_id",
        "pii_consent",
        "company",
        "department",
        "job_role",
        "tenure_years",
        "name",
        "phone",
        "email",
        "hashed_contact",  # 식별용 해시
        "extra_meta",  # 추가 메타데이터 (JSON)
        "created_at",
    ],
    # 6) Insights - 인사이트 결과 저장 (대시보드용)
    "Insights": [
        "insight_id",
        "course_id",  # 단일 과정 또는 cross-course용은 Null 허용
        "insight_scope",  # per_course / cross_course
        "insight_type",  # KPI / Trend / Finding / Quote
        "title",
        "description",
        "metric_name",
        "metric_value",
        "metric_unit",
        "breakdown_dim",  # 세분화 차원 (예: job_role)
        "breakdown_value",  # 세분화 값 (예: 엔지니어)
        "period_start",
        "period_end",
        "method",  # 집계 공식/정의
        "chart_spec_json",  # 시각화 스펙 (Vega-Lite 등)
        "source_query",  # 원천 쿼리/공식
        "last_updated",
    ],
    # 7) Lookups - 표준값 사전
    "Lookups": [
        "key",
        "value",
        "description",
    ],
    # 하위 호환을 위한 레거시 시트 (선택사항)
    "SurveySettings": [
        "courseId",
        "isActive",
        "startDate",
        "endDate",
        "maxResponses",
    ],
}


def _get_credentials(service_account_file: Optional[str] = None) -> Credentials:
    """서비스 계정 인증 정보 가져오기 (Streamlit Cloud & 로컬 지원)"""
    try:
        import streamlit as st
        
        # 1. Streamlit Secrets에서 JSON 문자열로 읽기 (Streamlit Cloud)
        if "GOOGLE_CREDENTIALS" in st.secrets:
            creds_dict = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return creds
    except Exception:
        pass  # Secrets 실패 시 로컬 파일로 fallback
    
    # 2. 로컬 파일 사용 (개발 환경)
    file_path = service_account_file or os.getenv(
        SERVICE_ACCOUNT_FILE_ENV_KEY, "huhsame-service-account-key.json"
    )
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    
    if os.path.exists(file_path):
        creds = Credentials.from_service_account_file(file_path, scopes=scopes)
        
        # Refresh token if needed
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        
        return creds
    
    # 3. 둘 다 실패
    raise FileNotFoundError(
        "서비스 계정 인증 정보를 찾을 수 없습니다.\n"
        "Streamlit Cloud: Secrets에 'GOOGLE_CREDENTIALS' 설정 필요.\n"
        "로컬: JSON 파일 경로 확인 필요."
    )


def get_client(service_account_file: Optional[str] = None) -> gspread.Client:
    creds = _get_credentials(service_account_file)
    return gspread.authorize(creds)


def open_or_create_spreadsheet(
    client: gspread.Client,
    title: str = "교육설문_시스템",
    spreadsheet_id: str | None = None,
) -> gspread.Spreadsheet:
    """Open an existing spreadsheet by ID or title, or create if permitted.

    Note: If no ID is supplied and Drive quota prevents creation, a RuntimeError is raised
    with guidance to set GOOGLE_SHEETS_SPREADSHEET_ID.
    """
    # Prefer explicit ID (param) then env var
    spreadsheet_id = spreadsheet_id or os.getenv(SPREADSHEET_ENV_KEY)
    if spreadsheet_id:
        return client.open_by_key(spreadsheet_id)
    # Try open by title if exists
    try:
        return client.open(title)
    except gspread.SpreadsheetNotFound:
        # As a last resort, attempt creation but handle quota/permission errors clearly
        try:
            ss = client.create(title)
            return ss
        except Exception as e:
            raise RuntimeError(
                "Cannot create spreadsheet (likely Drive quota/permission). "
                "Please set GOOGLE_SHEETS_SPREADSHEET_ID to an existing Sheet ID."
            ) from e


def ensure_schema(spreadsheet: gspread.Spreadsheet) -> Dict[str, gspread.Worksheet]:
    """Ensure all required worksheets exist with headers.

    Returns a mapping of sheet name to worksheet.
    """
    worksheets: Dict[str, gspread.Worksheet] = {}
    existing = {ws.title: ws for ws in spreadsheet.worksheets()}

    for sheet_name, headers in REQUIRED_SHEETS.items():
        if sheet_name in existing:
            ws = existing[sheet_name]
        else:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=max(10, len(headers)))
        worksheets[sheet_name] = ws
        # Set headers if first row is empty or different length
        current = ws.row_values(1)
        if not current or len(current) < len(headers):
            ws.resize(rows=1000, cols=max(10, len(headers)))
            ws.update("1:1", [headers])
    # Remove default empty sheet if not in REQUIRED_SHEETS
    if "Sheet1" in existing and "Sheet1" not in REQUIRED_SHEETS:
        try:
            spreadsheet.del_worksheet(existing["Sheet1"])
        except Exception:
            pass
    return worksheets


def upsert_course(
    spreadsheet: gspread.Spreadsheet,
    course: Dict[str, str],
) -> None:
    ws = spreadsheet.worksheet("Courses")
    headers = REQUIRED_SHEETS["Courses"]
    all_rows = ws.get_all_records()
    # Update by courseId if exists, else append
    target_index = None
    for idx, row in enumerate(all_rows, start=2):  # header is row 1
        if str(row.get("courseId")) == str(course.get("courseId")):
            target_index = idx
            break
    values = [course.get(col, "") for col in headers]
    if target_index is None:
        ws.append_row(values, value_input_option="USER_ENTERED")
    else:
        cell_range = f"{target_index}:{target_index}"
        ws.update(cell_range, [values])


def list_courses(spreadsheet: gspread.Spreadsheet) -> List[Dict[str, str]]:
    ws = spreadsheet.worksheet("Courses")
    return ws.get_all_records()


def get_survey_settings(spreadsheet: gspread.Spreadsheet, course_id: str) -> Dict[str, str]:
    ws = spreadsheet.worksheet("SurveySettings")
    records = ws.get_all_records()
    for idx, row in enumerate(records, start=2):
        if str(row.get("courseId")) == str(course_id):
            row["_row"] = idx
            return row
    return {"courseId": course_id, "isActive": "FALSE", "startDate": "", "endDate": "", "maxResponses": ""}


def set_survey_active(spreadsheet: gspread.Spreadsheet, course_id: str, is_active: bool) -> None:
    ws = spreadsheet.worksheet("SurveySettings")
    headers = REQUIRED_SHEETS["SurveySettings"]
    records = ws.get_all_records()
    target_index = None
    for idx, row in enumerate(records, start=2):
        if str(row.get("courseId")) == str(course_id):
            target_index = idx
            break
    values = [
        course_id,
        "TRUE" if is_active else "FALSE",
        "",
        "",
        "",
    ]
    if target_index is None:
        ws.append_row(values, value_input_option="USER_ENTERED")
    else:
        ws.update(f"{target_index}:{target_index}", [values])


def list_questions(spreadsheet: gspread.Spreadsheet, course_id: str) -> List[Dict[str, str]]:
    ws = spreadsheet.worksheet("Questions")
    records = ws.get_all_records()
    filtered = [r for r in records if str(r.get("courseId")) == str(course_id)]
    # sort by order numeric if present
    try:
        filtered.sort(key=lambda r: int(str(r.get("order", "0") or 0)))
    except Exception:
        pass
    return filtered


def upsert_question(spreadsheet: gspread.Spreadsheet, question: Dict[str, str]) -> None:
    ws = spreadsheet.worksheet("Questions")
    headers = REQUIRED_SHEETS["Questions"]
    records = ws.get_all_records()
    target_index = None
    for idx, row in enumerate(records, start=2):
        if str(row.get("questionId")) == str(question.get("questionId")):
            target_index = idx
            break
    values = [question.get(col, "") for col in headers]
    if target_index is None:
        ws.append_row(values, value_input_option="USER_ENTERED")
    else:
        ws.update(f"{target_index}:{target_index}", [values])


def save_response(spreadsheet: gspread.Spreadsheet, course_id: str, question_id: str, answer: str, respondent_hash: str, session_id: str, ip_masked: str) -> None:
    """Save a single response to the Responses sheet"""
    ws = spreadsheet.worksheet("Responses")
    headers = REQUIRED_SHEETS["Responses"]
    response_id = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    values = [
        response_id,
        course_id,
        question_id,
        answer,
        datetime.now(timezone.utc).isoformat(),
        respondent_hash,
        session_id,
        ip_masked,
    ]
    ws.append_row(values, value_input_option="USER_ENTERED")


def update_response_stats(spreadsheet: gspread.Spreadsheet, course_id: str) -> None:
    """Update ResponseStats for a course (v2 compatible)"""
    try:
        ws_responses = spreadsheet.worksheet("Responses")
        
        # v2 스키마: Survey_Items와 Course_Survey_Items 시트 사용
        try:
            ws_items = spreadsheet.worksheet("Survey_Items")
            ws_course_items = spreadsheet.worksheet("Course_Survey_Items")
            use_v2 = True
        except Exception:
            # 레거시: Questions 시트 사용
            ws_questions = spreadsheet.worksheet("Questions")
            use_v2 = False
        
        # ResponseStats 시트 확인
        try:
            ws_stats = spreadsheet.worksheet("ResponseStats")
        except Exception:
            # ResponseStats 시트가 없으면 생성하지 않고 종료
            return
        
        # Count unique respondents for this course
        responses = ws_responses.get_all_records()
        course_responses = [r for r in responses if str(r.get("course_id", r.get("courseId"))) == str(course_id)]
        unique_respondents = len(set(r.get("respondent_id", r.get("respondentHash")) for r in course_responses))
        
        # Count total questions/items for this course
        if use_v2:
            # v2: Course_Survey_Items에서 이 course에 매핑된 item 수 세기
            course_items = ws_course_items.get_all_records()
            total_questions = len([ci for ci in course_items if str(ci.get("course_id")) == str(course_id)])
        else:
            # 레거시: Questions에서 courseId로 필터링
            questions = ws_questions.get_all_records()
            course_questions = [q for q in questions if str(q.get("courseId")) == str(course_id)]
            total_questions = len(course_questions)
        
        # Calculate response rate
        response_rate = (unique_respondents / max(1, total_questions)) * 100 if total_questions > 0 else 0
        
        # Update or create stats record
        stats_records = ws_stats.get_all_records()
        target_index = None
        for idx, row in enumerate(stats_records, start=2):
            if str(row.get("courseId", row.get("course_id"))) == str(course_id):
                target_index = idx
                break
        
        values = [
            course_id,
            str(total_questions),
            str(unique_respondents),
            f"{response_rate:.1f}",
            datetime.now(timezone.utc).isoformat(),
        ]
        
        if target_index is None:
            ws_stats.append_row(values, value_input_option="USER_ENTERED")
        else:
            ws_stats.update(f"{target_index}:{target_index}", [values])
    except Exception as e:
        # 통계 업데이트 실패는 무시 (메인 업로드에 영향 없음)
        pass


def delete_question(spreadsheet: gspread.Spreadsheet, question_id: str) -> bool:
    ws = spreadsheet.worksheet("Questions")
    records = ws.get_all_records()
    for idx, row in enumerate(records, start=2):
        if str(row.get("questionId")) == str(question_id):
            ws.delete_rows(idx)
            return True
    return False


def get_course_by_id(spreadsheet: gspread.Spreadsheet, course_id: str) -> Dict[str, str]:
    """Get a specific course by ID (LEGACY)"""
    courses = list_courses(spreadsheet)
    for course in courses:
        if str(course.get("courseId")) == str(course_id):
            return course
    return {}


def get_course_by_id_v2(spreadsheet: gspread.Spreadsheet, course_id: str) -> Dict[str, str]:
    """v2 스키마: course_id로 과정 조회"""
    try:
        ws = spreadsheet.worksheet("Courses")
        all_rows = ws.get_all_records()
        
        for row in all_rows:
            if str(row.get("course_id", "")).strip() == str(course_id).strip():
                return row
        return {}
    except Exception as e:
        print(f"Error loading course {course_id}: {e}")
        return {}


def get_responses_for_course(spreadsheet: gspread.Spreadsheet, course_id: str) -> List[Dict[str, str]]:
    """Get all responses for a specific course"""
    ws = spreadsheet.worksheet("Responses")
    responses = ws.get_all_records()
    return [r for r in responses if str(r.get("courseId")) == str(course_id)]


def get_responses_by_question(spreadsheet: gspread.Spreadsheet, course_id: str, question_id: str) -> List[Dict[str, str]]:
    """Get all responses for a specific question"""
    ws = spreadsheet.worksheet("Responses")
    responses = ws.get_all_records()
    return [r for r in responses if str(r.get("courseId")) == str(course_id) and str(r.get("questionId")) == str(question_id)]


def save_analysis(spreadsheet: gspread.Spreadsheet, course_id: str, analysis_data: Dict[str, str]) -> None:
    """Save AI analysis results to Analysis sheet (LEGACY - use save_insight for new schema)"""
    # Legacy function kept for backward compatibility
    pass


# ============================================================================
# NEW SCHEMA FUNCTIONS (개선된 스키마 전용 함수들)
# ============================================================================

def upsert_course_v2(spreadsheet: gspread.Spreadsheet, course: Dict[str, str]) -> None:
    """새 스키마: 과정 정보 저장/업데이트"""
    ws = spreadsheet.worksheet("Courses")
    headers = REQUIRED_SHEETS["Courses"]
    all_rows = ws.get_all_records()
    
    # course_id 문자열 강제 변환 (절대 날짜/시간으로 변환하지 않음)
    course["course_id"] = str(course.get("course_id", "")).strip()
    
    if not course["course_id"]:
        raise ValueError("course_id는 필수입니다. 빈 값을 저장할 수 없습니다.")
    
    # course_id로 기존 행 찾기
    target_index = None
    for idx, row in enumerate(all_rows, start=2):
        if str(row.get("course_id")).strip() == course["course_id"]:
            target_index = idx
            break
    
    # 값 준비 (모든 값을 문자열로 변환)
    values = [str(course.get(col, "")) for col in headers]
    
    if target_index is None:
        # 새 행 추가
        ws.append_row(values, value_input_option="USER_ENTERED")
    else:
        # 기존 행 업데이트 (course_id는 절대 변경되지 않음)
        ws.update(f"{target_index}:{target_index}", [values])


def list_courses_v2(spreadsheet: gspread.Spreadsheet, status: str = None) -> List[Dict[str, str]]:
    """새 스키마: 과정 목록 조회 (status 필터 옵션)"""
    ws = spreadsheet.worksheet("Courses")
    records = ws.get_all_records()
    if status:
        return [r for r in records if str(r.get("status")) == str(status)]
    return records


def upsert_survey_item(spreadsheet: gspread.Spreadsheet, item: Dict[str, str]) -> None:
    """새 스키마: 설문 항목 저장/업데이트 (표준 문항 카탈로그)"""
    ws = spreadsheet.worksheet("Survey_Items")
    headers = REQUIRED_SHEETS["Survey_Items"]
    all_rows = ws.get_all_records()
    
    # item_id로 기존 행 찾기
    target_index = None
    for idx, row in enumerate(all_rows, start=2):
        if str(row.get("item_id")) == str(item.get("item_id")):
            target_index = idx
            break
    
    values = [item.get(col, "") for col in headers]
    
    if target_index is None:
        ws.append_row(values, value_input_option="USER_ENTERED")
    else:
        ws.update(f"{target_index}:{target_index}", [values])


def list_survey_items(spreadsheet: gspread.Spreadsheet, is_active: bool = True) -> List[Dict[str, str]]:
    """새 스키마: 설문 항목 목록 조회"""
    ws = spreadsheet.worksheet("Survey_Items")
    records = ws.get_all_records()
    if is_active:
        return [r for r in records if str(r.get("is_active")).upper() in ["TRUE", "1", "Y"]]
    return records


def get_survey_item_by_code(spreadsheet: gspread.Spreadsheet, item_code: str) -> Dict[str, str]:
    """새 스키마: item_code로 표준 문항 조회"""
    ws = spreadsheet.worksheet("Survey_Items")
    records = ws.get_all_records()
    for r in records:
        if str(r.get("item_code")) == str(item_code):
            return r
    return {}


def map_item_to_course(spreadsheet: gspread.Spreadsheet, course_id: str, item_id: str, 
                       order: int = 0, is_required: bool = False, custom_text: str = "") -> None:
    """새 스키마: 과정에 문항 매핑 (재사용 가능)"""
    ws = spreadsheet.worksheet("Course_Item_Map")
    headers = REQUIRED_SHEETS["Course_Item_Map"]
    
    map_id = f"{course_id}_{item_id}"
    values = [
        map_id,
        course_id,
        item_id,
        str(order),
        "TRUE" if is_required else "FALSE",
        custom_text,
        datetime.now(timezone.utc).isoformat(),
    ]
    ws.append_row(values, value_input_option="USER_ENTERED")


def get_course_items(spreadsheet: gspread.Spreadsheet, course_id: str) -> List[Dict[str, str]]:
    """새 스키마: 특정 과정의 문항 목록 조회 (매핑 + 문항 정보)"""
    ws_map = spreadsheet.worksheet("Course_Item_Map")
    ws_items = spreadsheet.worksheet("Survey_Items")
    
    mappings = ws_map.get_all_records()
    items = ws_items.get_all_records()
    
    # course_id에 해당하는 매핑만 필터
    course_mappings = [m for m in mappings if str(m.get("course_id")) == str(course_id)]
    
    # item_id로 문항 정보 병합
    result = []
    for mapping in course_mappings:
        item_id = str(mapping.get("item_id"))
        item_info = next((i for i in items if str(i.get("item_id")) == item_id), {})
        
        # 매핑 정보 + 문항 정보 합치기
        combined = {**item_info, **mapping}
        result.append(combined)
    
    # order_in_course로 정렬
    try:
        result.sort(key=lambda x: int(str(x.get("order_in_course", "0") or 0)))
    except Exception:
        pass
    
    return result


def save_response_v2(spreadsheet: gspread.Spreadsheet, response: Dict[str, str]) -> None:
    """새 스키마: 응답 저장 (정규화된 형식)
    
    🚨 핵심: Responses 시트의 헤더 순서에 맞춰 데이터를 저장하여 데이터 밀림 방지
    
    물리적 열 순서 (REQUIRED_SHEETS["Responses"]):
      1. response_id     - 응답 고유 ID
      2. course_id       - 과정 ID
      3. respondent_id   - 응답자 ID
      4. timestamp       - 응답 시각 (ISO 8601)
      5. item_id         - 문항 ID (I-xxxxxxxx) ⚠️ 중요: 이 값이 타임스탬프와 바뀌면 안 됨!
      6. response_value  - 응답 값 (텍스트)
      7. response_value_num - 응답 값 (숫자)
      8. choice_value    - 선택지 값
      9. comment_text    - 코멘트/주관식
     10. source_row_index - 원본 파일 행 번호
     11. ingest_batch_id  - 배치 ID
    """
    ws = spreadsheet.worksheet("Responses")
    headers = REQUIRED_SHEETS["Responses"]
    
    # response_id가 없으면 자동 생성
    if not response.get("response_id"):
        response["response_id"] = str(int(datetime.now(timezone.utc).timestamp() * 1000000))
    
    # 🔑 명시적 순서 보장: headers 리스트 순서대로 값을 추출
    # headers = ["response_id", "course_id", "respondent_id", "timestamp", "item_id", ...]
    ordered_values = [response.get(col, "") for col in headers]
    
    # ⚠️ 데이터 정합성 검증 (디버그용)
    if len(ordered_values) != len(headers):
        raise ValueError(f"데이터 길이 불일치: expected {len(headers)}, got {len(ordered_values)}")
    
    ws.append_row(ordered_values, value_input_option="USER_ENTERED")


def save_respondent(spreadsheet: gspread.Spreadsheet, respondent: Dict[str, str]) -> None:
    """새 스키마: 응답자 정보 저장 (PII 분리)"""
    ws = spreadsheet.worksheet("Respondents")
    headers = REQUIRED_SHEETS["Respondents"]
    all_rows = ws.get_all_records()
    
    # respondent_id로 기존 행 찾기 (중복 방지)
    target_index = None
    for idx, row in enumerate(all_rows, start=2):
        if str(row.get("respondent_id")) == str(respondent.get("respondent_id")):
            target_index = idx
            break
    
    values = [respondent.get(col, "") for col in headers]
    
    if target_index is None:
        ws.append_row(values, value_input_option="USER_ENTERED")
    else:
        ws.update(f"{target_index}:{target_index}", [values])


def get_responses_v2(spreadsheet: gspread.Spreadsheet, course_id: str = None, 
                     item_id: str = None, respondent_id: str = None) -> List[Dict[str, str]]:
    """새 스키마: 응답 조회 (다양한 필터 옵션)"""
    ws = spreadsheet.worksheet("Responses")
    responses = ws.get_all_records()
    
    # 필터 적용
    if course_id:
        responses = [r for r in responses if str(r.get("course_id")) == str(course_id)]
    if item_id:
        responses = [r for r in responses if str(r.get("item_id")) == str(item_id)]
    if respondent_id:
        responses = [r for r in responses if str(r.get("respondent_id")) == str(respondent_id)]
    
    return responses


def save
def save_insight(spreadsheet: gspread.Spreadsheet, insight: Dict[str, str]) -> None:
    """새 스키마: 인사이트 저장 (대시보드용)"""
    ws = spreadsheet.worksheet("Insights")
    headers = REQUIRED_SHEETS["Insights"]
    
    # insight_id가 없으면 자동 생성
    if not insight.get("insight_id"):
        insight["insight_id"] = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    
    values = [insight.get(col, "") for col in headers]
    ws.append_row(values, value_input_option="USER_ENTERED")


def get_insights(spreadsheet: gspread.Spreadsheet, course_id: str = None, 
                 insight_scope: str = None, insight_type: str = None) -> List[Dict[str, str]]:
    """새 스키마: 인사이트 조회 (필터 옵션)"""
    ws = spreadsheet.worksheet("Insights")
    insights = ws.get_all_records()
    
    # 필터 적용
    if course_id:
        insights = [i for i in insights if str(i.get("course_id")) == str(course_id)]
    if insight_scope:
        insights = [i for i in insights if str(i.get("insight_scope")) == str(insight_scope)]
    if insight_type:
        insights = [i for i in insights if str(i.get("insight_type")) == str(insight_type)]
    
    return insights


def upsert_lookup(spreadsheet: gspread.Spreadsheet, key: str, value: str, description: str = "") -> None:
    """새 스키마: 표준값 사전 저장/업데이트"""
    ws = spreadsheet.worksheet("Lookups")
    all_rows = ws.get_all_records()
    
    # key로 기존 행 찾기
    target_index = None
    for idx, row in enumerate(all_rows, start=2):
        if str(row.get("key")) == str(key):
            target_index = idx
            break
    
    values = [key, value, description]
    
    if target_index is None:
        ws.append_row(values, value_input_option="USER_ENTERED")
    else:
        ws.update(f"{target_index}:{target_index}", [values])


def get_lookups(spreadsheet: gspread.Spreadsheet) -> Dict[str, str]:
    """새 스키마: 표준값 사전 조회 (key-value 딕셔너리 반환)"""
    ws = spreadsheet.worksheet("Lookups")
    records = ws.get_all_records()
    return {str(r.get("key")): str(r.get("value")) for r in records}


def initialize_standard_lookups(spreadsheet: gspread.Spreadsheet) -> None:
    """새 스키마: 표준값 사전 초기화 (event_type, metric_type, dimension 등)"""
    standard_values = [
        # event_type
        ("event_type.nct", "NCT", "Next Chip Talk 세미나"),
        ("event_type.forum", "Forum", "미래반도체 포럼"),
        ("event_type.workshop", "Workshop", "워크샵"),
        ("event_type.webinar", "Webinar", "웨비나"),
        ("event_type.internal_talk", "Internal Talk", "사내 강연"),
        
        # metric_type
        ("metric_type.likert", "likert", "리커트 척도 (1-5, 1-7 등)"),
        ("metric_type.nps", "nps", "Net Promoter Score"),
        ("metric_type.single_choice", "single_choice", "단일 선택"),
        ("metric_type.multi_choice", "multi_choice", "복수 선택"),
        ("metric_type.text", "text", "주관식 텍스트"),
        
        # dimension
        ("dimension.satisfaction", "satisfaction", "만족도"),
        ("dimension.difficulty", "difficulty", "난이도"),
        ("dimension.understanding", "understanding", "이해도"),
        ("dimension.insight", "insight", "인사이트"),
        ("dimension.recommend", "recommend", "추천도"),
        ("dimension.operations", "operations", "운영/진행"),
        ("dimension.content", "content", "콘텐츠/내용"),
        ("dimension.nps", "nps", "순추천지수"),
        
        # status
        ("status.planned", "planned", "계획됨"),
        ("status.active", "active", "진행중"),
        ("status.completed", "completed", "완료"),
        ("status.archived", "archived", "보관"),
        
        # insight_type
        ("insight_type.kpi", "KPI", "주요 지표"),
        ("insight_type.trend", "Trend", "추세 분석"),
        ("insight_type.finding", "Finding", "주요 발견사항"),
        ("insight_type.quote", "Quote", "인용/피드백"),
        
        # insight_scope
        ("insight_scope.per_course", "per_course", "단일 과정"),
        ("insight_scope.cross_course", "cross_course", "과정간 비교"),
    ]
    
    for key, value, description in standard_values:
        upsert_lookup(spreadsheet, key, value, description)


def initialize_standard_items(spreadsheet: gspread.Spreadsheet) -> None:
    """새 스키마: 표준 설문 항목 초기화 (재사용 가능한 템플릿 문항)"""
    standard_items = [
        {
            "item_id": "ITEM_SAT_OVERALL",
            "item_code": "SAT_OVERALL",
            "item_group": "전체평가",
            "item_text": "전반적으로 이번 교육에 만족하셨나요?",
            "metric_type": "likert",
            "dimension": "satisfaction",
            "scale_min": "1",
            "scale_max": "5",
            "scale_label_min": "매우 불만족",
            "scale_label_max": "매우 만족",
            "options": "",
            "applies_to_speaker": "",
            "applies_to_session": "",
            "default_order": "100",
            "is_active": "TRUE",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "item_id": "ITEM_DIFF_OVERALL",
            "item_code": "DIFF_OVERALL",
            "item_group": "전체평가",
            "item_text": "교육 내용의 난이도는 어떠셨나요?",
            "metric_type": "likert",
            "dimension": "difficulty",
            "scale_min": "1",
            "scale_max": "5",
            "scale_label_min": "매우 쉬움",
            "scale_label_max": "매우 어려움",
            "options": "",
            "applies_to_speaker": "",
            "applies_to_session": "",
            "default_order": "200",
            "is_active": "TRUE",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "item_id": "ITEM_UNDERSTAND_OVERALL",
            "item_code": "UNDERSTAND_OVERALL",
            "item_group": "전체평가",
            "item_text": "교육 내용을 얼마나 이해하셨나요?",
            "metric_type": "likert",
            "dimension": "understanding",
            "scale_min": "1",
            "scale_max": "5",
            "scale_label_min": "전혀 이해 못함",
            "scale_label_max": "완전히 이해함",
            "options": "",
            "applies_to_speaker": "",
            "applies_to_session": "",
            "default_order": "300",
            "is_active": "TRUE",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "item_id": "ITEM_NPS",
            "item_code": "NPS",
            "item_group": "추천도",
            "item_text": "이 교육을 동료에게 추천하시겠습니까? (0-10점)",
            "metric_type": "nps",
            "dimension": "nps",
            "scale_min": "0",
            "scale_max": "10",
            "scale_label_min": "전혀 추천안함",
            "scale_label_max": "매우 추천함",
            "options": "",
            "applies_to_speaker": "",
            "applies_to_session": "",
            "default_order": "900",
            "is_active": "TRUE",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "item_id": "ITEM_FEEDBACK_TEXT",
            "item_code": "FEEDBACK_TEXT",
            "item_group": "주관식",
            "item_text": "개선사항이나 의견을 자유롭게 작성해주세요.",
            "metric_type": "text",
            "dimension": "content",
            "scale_min": "",
            "scale_max": "",
            "scale_label_min": "",
            "scale_label_max": "",
            "options": "",
            "applies_to_speaker": "",
            "applies_to_session": "",
            "default_order": "1000",
            "is_active": "TRUE",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    ]
    
    for item in standard_items:
        upsert_survey_item(spreadsheet, item)


# ============================================================================
# 헤더 기반 문항 자동 등록 (Auto Item Registration from Headers)
# ============================================================================

def slugify(text: str) -> str:
    """텍스트를 slug로 변환 (한글/영문 모두 지원)"""
    text = str(text).strip().lower()
    # 특수문자 제거
    text = re.sub(r'[^\w\s가-힣-]', '', text)
    # 공백을 언더스코어로
    text = re.sub(r'[\s]+', '_', text)
    return text[:30]  # 최대 30자


def generate_item_code(item_text: str, dimension: Optional[str], metric_type: str) -> str:
    """항목 코드 생성 (중복 방지용 고유 코드)"""
    base = dimension or metric_type or "item"
    slug = slugify(item_text)
    hash_str = hashlib.md5(item_text.encode('utf-8')).hexdigest()[:6]
    return f"{slugify(base)}_{slug}_{hash_str}".upper()


def generate_item_id() -> str:
    """새 항목 ID 생성"""
    import uuid
    return f"I-{uuid.uuid4().hex[:8].upper()}"


def generate_map_id() -> str:
    """새 매핑 ID 생성"""
    import uuid
    return f"M-{uuid.uuid4().hex[:8].upper()}"


def is_survey_question(header: str) -> bool:
    """헤더가 설문 문항인지 판단 (메타데이터 제외)"""
    header_lower = str(header).strip().lower()
    
    # 🚨 핵심 수정: 회사/소속/직군/연차 등은 설문 문항으로 포함
    # 메타데이터이지만 분석 가치가 있는 항목들
    include_metadata_keywords = ["회사", "소속", "부서", "직무", "직군", "직책", "연차", "company", "department", "job"]
    for keyword in include_metadata_keywords:
        if keyword in header_lower:
            return True  # 설문 문항으로 포함
    
    # PII 항목만 제외 (개인식별정보)
    exclude_pii_keywords = [
        'timestamp', '타임스탬프', '시간', '날짜', 'date',
        'email', '이메일', '메일',
        'name', '이름', '성명',
        'phone', '전화', '연락처',
        'id', 'user_id', 'respondent_id',
    ]
    
    for keyword in exclude_pii_keywords:
        if keyword in header_lower:
            return False
    
    # 너무 짧은 헤더는 제외
    if len(header.strip()) < 3:
        return False
    
    return True


def guess_metric_type_and_dimension(header: str) -> Tuple[str, Optional[str], int, int]:
    """
    헤더에서 metric_type, dimension, scale_min, scale_max 추론
    
    Returns:
        (metric_type, dimension, scale_min, scale_max)
    """
    header_lower = str(header).strip().lower()
    
    # 🚨 핵심 수정: 메타데이터성 항목을 'text' 타입으로 강제 인식
    # "소속 회사", "직군", "연차", "회사명" 같은 항목은 주관식 텍스트로 수집
    metadata_text_keywords = ["직군", "연차", "회사명", "회사", "소속", "부서", "직무", "직책"]
    for keyword in metadata_text_keywords:
        if keyword in header_lower:
            return ('text', None, 0, 0)
    
    # NPS 패턴
    if any(keyword in header_lower for keyword in ['추천', 'nps', 'recommend', '0~10', '0-10']):
        return ('nps', 'recommend', 0, 10)
    
    # Likert scale 패턴
    likert_patterns = [
        (r'[1-5]점', (1, 5)),
        (r'5점\s*만점', (1, 5)),
        (r'[1-7]점', (1, 7)),
        (r'7점\s*만점', (1, 7)),
    ]
    for pattern, (min_val, max_val) in likert_patterns:
        if re.search(pattern, header):
            dimension = infer_dimension_from_text(header)
            return ('likert', dimension, min_val, max_val)
    
    # Dimension 키워드로 Likert 추론
    dimension_keywords = {
        'satisfaction': ['만족', '만족도'],
        'difficulty': ['난이', '난이도', '어려움'],
        'understanding': ['이해', '이해도'],
        'insight': ['인사이트', '도움', '유익'],
        'operations': ['운영', '진행', '장소', '시설'],
        'content': ['내용', '구성', '주제'],
    }
    
    for dim, keywords in dimension_keywords.items():
        if any(kw in header_lower for kw in keywords):
            return ('likert', dim, 1, 5)
    
    # Yes/No 패턴
    if any(keyword in header_lower for keyword in ['예/아니오', 'yes/no', '동의', '참석']):
        return ('single_choice', None, 0, 0)
    
    # 복수 선택 패턴
    if any(keyword in header_lower for keyword in ['복수', '모두', '해당되는', 'multiple']):
        return ('multi_choice', None, 0, 0)
    
    # 기본: text (서술형)
    return ('text', None, 0, 0)


def infer_dimension_from_text(text: str) -> Optional[str]:
    """텍스트에서 dimension 추론"""
    text_lower = str(text).strip().lower()
    
    dimension_keywords = {
        'satisfaction': ['만족', '만족도'],
        'difficulty': ['난이', '난이도', '어려움'],
        'understanding': ['이해', '이해도'],
        'insight': ['인사이트', '도움', '유익', '도움'],
        'recommend': ['추천', 'nps', 'recommend'],
        'operations': ['운영', '진행', '장소', '시설', '안내'],
        'content': ['내용', '구성', '주제', '강의'],
    }
    
    for dim, keywords in dimension_keywords.items():
        if any(kw in text_lower for kw in keywords):
            return dim
    
    return None


def extract_session_number(text: str) -> Optional[str]:
    """텍스트에서 세션 번호 추출"""
    # 패턴: Session 1, 세션1, 세션 2
    patterns = [
        r'\bSession\s*(\d+)\b',
        r'세션\s*(\d+)',
        r'\[세션\s*(\d+)\]',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None


def extract_speaker_name(text: str) -> Optional[str]:
    """텍스트에서 발표자 이름 추출"""
    # 패턴: [고영민], 김현재, (박종경)
    patterns = [
        r'[\[\(]([가-힣]{2,4})[\]\)]',  # 괄호 안의 한글 이름
        r'\b([가-힣]{2,4})\s*(?:박사|교수|님|연구원|대표)',  # 직함 앞의 이름
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    
    return None


def infer_item_from_header(header: str, order: int) -> Dict:
    """
    헤더로부터 Survey_Items 항목 정보 추론
    
    Args:
        header: 컬럼 헤더명
        order: 순서 (0부터 시작)
    
    Returns:
        항목 정보 딕셔너리
    """
    metric_type, dimension, scale_min, scale_max = guess_metric_type_and_dimension(header)
    session_no = extract_session_number(header)
    speaker = extract_speaker_name(header)
    
    # 옵션 설정 (single_choice, multi_choice)
    options = None
    if metric_type in ['single_choice', 'multi_choice']:
        # 기본 옵션 (실제 데이터에서 추출하는 것이 더 정확)
        if '예/아니오' in header.lower() or 'yes/no' in header.lower():
            options = '예,아니오'
        else:
            options = None  # 실제 데이터에서 추출 필요
    
    # Scale label 설정
    scale_label_min = None
    scale_label_max = None
    if metric_type == 'likert':
        scale_label_min = '매우 낮음'
        scale_label_max = '매우 높음'
    elif metric_type == 'nps':
        scale_label_min = '전혀 추천하지 않음'
        scale_label_max = '적극 추천'
    
    return {
        "item_text": header.strip(),
        "metric_type": metric_type,
        "dimension": dimension,
        "scale_min": scale_min if scale_min > 0 else None,
        "scale_max": scale_max if scale_max > 0 else None,
        "scale_label_min": scale_label_min,
        "scale_label_max": scale_label_max,
        "options": options,
        "applies_to_speaker": speaker,
        "applies_to_session": session_no,
        "default_order": order,
        "item_group": f"Session {session_no}" if session_no else None,
    }


def ensure_survey_items_from_headers(
    spreadsheet: gspread.Spreadsheet,
    headers: List[str]
) -> List[Dict]:
    """
    헤더 목록으로부터 Survey_Items 자동 등록 (중복 방지)
    
    Args:
        spreadsheet: Google Spreadsheet 객체
        headers: 컬럼 헤더 리스트
    
    Returns:
        등록된 항목 정보 리스트 (item_id 포함)
    """
    ws = spreadsheet.worksheet("Survey_Items")
    all_items = ws.get_all_records()
    
    # 기존 item_code 목록
    existing_codes = {str(row.get("item_code", "")) for row in all_items}
    
    result_items = []
    
    for idx, header in enumerate(headers):
        # 설문 문항인지 확인
        if not is_survey_question(header):
            continue
        
        # 항목 정보 추론
        item_info = infer_item_from_header(header, idx)
        
        # item_code 생성
        item_code = generate_item_code(
            item_info["item_text"],
            item_info.get("dimension"),
            item_info["metric_type"]
        )
        
        # 중복 확인
        if item_code in existing_codes:
            # 기존 항목 찾기
            for row in all_items:
                if str(row.get("item_code", "")) == item_code:
                    result_items.append(row)
                    break
            continue
        
        # 새 항목 생성
        item_id = generate_item_id()
        new_item = {
            "item_id": item_id,
            "item_code": item_code,
            "item_group": item_info.get("item_group") or "",
            "item_text": item_info["item_text"],
            "metric_type": item_info["metric_type"],
            "dimension": item_info.get("dimension") or "",
            "scale_min": item_info.get("scale_min") or "",
            "scale_max": item_info.get("scale_max") or "",
            "scale_label_min": item_info.get("scale_label_min") or "",
            "scale_label_max": item_info.get("scale_label_max") or "",
            "options": item_info.get("options") or "",
            "applies_to_speaker": item_info.get("applies_to_speaker") or "",
            "applies_to_session": item_info.get("applies_to_session") or "",
            "default_order": item_info["default_order"],
            "is_active": "TRUE",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # 저장
        headers_list = REQUIRED_SHEETS["Survey_Items"]
        values = [str(new_item.get(col, "")) for col in headers_list]
        ws.append_row(values, value_input_option="USER_ENTERED")
        
        result_items.append(new_item)
        existing_codes.add(item_code)
    
    return result_items


def ensure_course_item_mapping(
    spreadsheet: gspread.Spreadsheet,
    course_id: str,
    item_list: List[Dict]
) -> None:
    """
    Course와 Survey_Items 자동 매핑
    
    Args:
        spreadsheet: Google Spreadsheet 객체
        course_id: 과정 ID
        item_list: 항목 리스트 (item_id 포함)
    """
    ws = spreadsheet.worksheet("Course_Item_Map")
    all_maps = ws.get_all_records()
    
    # 기존 매핑 확인
    existing_pairs = {
        (str(row.get("course_id", "")), str(row.get("item_id", "")))
        for row in all_maps
    }
    
    for item in item_list:
        item_id = str(item.get("item_id", ""))
        if not item_id:
            continue
        
        # 중복 확인
        if (course_id, item_id) in existing_pairs:
            continue
        
        # 새 매핑 생성
        map_id = generate_map_id()
        new_mapping = {
            "map_id": map_id,
            "course_id": course_id,
            "item_id": item_id,
            "order_in_course": item.get("default_order", ""),
            "is_required": "TRUE",
            "custom_item_text": "",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        
        headers_list = REQUIRED_SHEETS["Course_Item_Map"]
        values = [str(new_mapping.get(col, "")) for col in headers_list]
        ws.append_row(values, value_input_option="USER_ENTERED")


def delete_course_item_mappings(
    spreadsheet: gspread.Spreadsheet,
    course_id: str,
) -> int:
    """특정 course_id와 매핑된 Course_Item_Map 행 삭제"""

    ws = spreadsheet.worksheet("Course_Item_Map")
    all_values = ws.get_all_values()

    if not all_values:
        return 0

    header = all_values[0]
    try:
        course_idx = header.index("course_id")
    except ValueError:
        return 0

    rows_to_delete = []
    for idx, row in enumerate(all_values[1:], start=2):
        if len(row) <= course_idx:
            continue
        if str(row[course_idx]).strip() == str(course_id):
            rows_to_delete.append(idx)

    for row_num in reversed(rows_to_delete):
        ws.delete_rows(row_num)

    return len(rows_to_delete)
    
