#!/usr/bin/env python3
"""
NCT 설문 응답 데이터 강제 주입 스크립트

목적: 4개 회차의 NCT 설문 응답 데이터를 기존 Course에 맵핑하여 
      Responses 및 Respondents 시트에 주입

실행 방법:
    python inject_responses.py

주의사항:
    - 실행 전 Responses 및 Respondents 시트의 모든 데이터가 삭제됩니다
    - 백업 필요 시 스프레드시트를 먼저 복사하세요
"""

import os
import sys
import io
import hashlib
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple
import time

import pandas as pd
import gspread

# 로컬 모듈 임포트
from gsheets_utils import (
    get_client,
    open_or_create_spreadsheet,
    save_respondent,
    save_response_v2,
    ensure_survey_items_from_headers,
    ensure_course_item_mapping,
    delete_course_item_mappings,
)
from survey_app import normalize_company_name, generate_response_id, generate_respondent_id, generate_batch_id


# ============================================================================
# 설정: Course ID와 파일명 맵핑
# ============================================================================

COURSE_FILE_MAPPING = [
    {
        "course_id": "CARD-1c9x",
        "file_path": "교육설문대쉬보드/NCT 1회차 설문지.csv",
        "description": "Next Chip Talk 1회차 - 빛이 여는 반도체의 미래"
    },
    {
        "course_id": "CARD-1cqk",
        "file_path": "교육설문대쉬보드/NCT 2회차 설문지.csv",
        "description": "Next Chip Talk 2회차 - 반도체, 유리를 품다"
    },
    {
        "course_id": "CARD-1cyn",
        "file_path": "교육설문대쉬보드/NCT 3회차 설문지.csv",
        "description": "Next Chip Talk 3회차 - AI의 뇌를 설계하다"
    },
    {
        "course_id": "CARD-1ddq",
        "file_path": "교육설문대쉬보드/NCT 4회차 설문지.csv",
        "description": "Next Chip Talk 4회차 - Next AND Necessity About New Direction"
    },
]


# PII/메타데이터 열 매핑 (헤더 텍스트 -> Respondents 필드)
PII_COLUMN_MAPPING = {
    "소속 회사": "company",
    "소속 회사명을 작성해주세요": "company",
    "소속 회사명을 작성해주세요.": "company",
    "직군": "job_role",
    "본인의 직군을 선택해주세요": "job_role",
    "본인의 직군을 선택해주세요.": "job_role",
    "연차": "tenure_years",
    "본인의 연차를 선택해주세요": "tenure_years",
    "본인의 연차를 선택해주세요.": "tenure_years",
    "성함": "name",
    "성함을 작성해주세요": "name",
    "성함을 작성해주세요.": "name",
    "전화번호": "phone",
    "전화번호를 작성해주세요": "phone",
}


# ============================================================================
# 시트 클린징 함수
# ============================================================================

def clear_sheet_data(spreadsheet: gspread.Spreadsheet, sheet_name: str):
    """
    특정 시트의 모든 데이터 행을 삭제 (헤더 제외)
    
    Args:
        spreadsheet: Google Spreadsheet 객체
        sheet_name: 클린징할 시트 이름
    """
    try:
        ws = spreadsheet.worksheet(sheet_name)
        all_values = ws.get_all_values()
        
        if len(all_values) <= 1:
            print(f"   ✅ {sheet_name}: 이미 비어있음 (헤더만 존재)")
            return
        
        # 헤더 제외한 모든 행 삭제
        num_rows_to_delete = len(all_values) - 1
        ws.delete_rows(2, len(all_values))
        
        print(f"   ✅ {sheet_name}: {num_rows_to_delete}개 행 삭제 완료")
        
    except Exception as e:
        print(f"   ⚠️ {sheet_name} 클린징 실패: {str(e)}")


# ============================================================================
# CSV/XLSX 파일 읽기 (인코딩 처리)
# ============================================================================

def read_response_file(file_path: str) -> pd.DataFrame:
    """
    CSV 또는 XLSX 파일을 읽어 DataFrame 반환
    
    Args:
        file_path: 파일 경로
        
    Returns:
        pd.DataFrame: 읽은 데이터
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
    
    # 🚨 핵심 수정: 파일 시그니처 먼저 확인 (확장자보다 우선)
    with open(file_path, 'rb') as f:
        magic = f.read(4)
    
    is_zip_based = magic[:2] == b'PK'  # ZIP/XLSX 시그니처
    
    # ZIP 기반 파일 (XLSX)이면 무조건 Excel로 읽기
    if is_zip_based:
        print(f"   💡 파일 시그니처 확인: XLSX 형식 (ZIP 기반)")
        try:
            df = pd.read_excel(file_path, header=0, dtype=str, engine='openpyxl')
            print(f"   ✅ Excel 파일 읽기 성공: {len(df)} 행")
            return df
        except Exception as e:
            print(f"   ❌ Excel 읽기 실패: {str(e)}")
            raise
    
    # ZIP 기반이 아니면 CSV로 시도
    print(f"   💡 파일 시그니처 확인: CSV 형식")
    encodings = ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8', 'latin-1']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, header=0, encoding=encoding, dtype=str)
            print(f"   ✅ CSV 파일 읽기 성공 ({encoding}): {len(df)} 행")
            return df
        except Exception:
            continue
    
    raise ValueError(f"파일을 읽을 수 없습니다: {file_path}")


# ============================================================================
# PII 추출 및 정규화
# ============================================================================

def extract_pii_from_row(row: pd.Series, header_to_field: Dict[str, str]) -> Dict[str, str]:
    """
    행에서 PII/메타데이터 추출
    
    Args:
        row: DataFrame 행
        header_to_field: 헤더 -> Respondents 필드 매핑
        
    Returns:
        Dict[str, str]: Respondents 필드 데이터
    """
    pii_data = {
        "company": "",
        "department": "",
        "job_role": "",
        "tenure_years": "",
        "name": "",
        "phone": "",
        "email": "",
    }
    
    for header, value in row.items():
        header_lower = str(header).lower().strip()
        
        # PII 매핑에서 찾기
        for pii_keyword, field_name in PII_COLUMN_MAPPING.items():
            if pii_keyword.lower() in header_lower:
                pii_data[field_name] = str(value).strip() if pd.notna(value) else ""
                break
    
    # 회사명 정규화
    if pii_data["company"]:
        pii_data["company"] = normalize_company_name(pii_data["company"])
    
    return pii_data


def is_pii_column(header: str) -> bool:
    """
    헤더가 PII/메타데이터 열인지 판단
    
    Args:
        header: 헤더 텍스트
        
    Returns:
        bool: PII 열이면 True
    """
    header_lower = header.lower().strip()
    
    pii_keywords = [
        "타임스탬프", "timestamp", "날짜", "date",
        "이름", "성함", "성명", "name",
        "전화", "연락처", "phone", "mobile",
        "이메일", "메일", "email",
        "소속", "회사", "company",
        "부서", "department",
        "직군", "직무", "직책", "job",
        "연차", "tenure",
    ]
    
    return any(keyword in header_lower for keyword in pii_keywords)


def normalize_header_text(text: str) -> str:
    """헤더 및 item_text 비교를 위한 정규화"""

    normalized = str(text or "").lower()
    normalized = normalized.replace('"', "")
    normalized = normalized.replace("“", "").replace("”", "")
    normalized = normalized.replace("'", "")
    normalized = normalized.replace("[", "").replace("]", "")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def build_header_item_mapping(
    question_headers: List[str],
    registered_items: List[Dict],
) -> Tuple[Dict[str, str], List[str]]:
    """헤더 텍스트를 item_id에 매핑"""

    header_lookup: Dict[str, str] = {}
    for header in question_headers:
        norm_header = normalize_header_text(header)
        header_lookup.setdefault(norm_header, header)

    header_to_item_id: Dict[str, str] = {}
    unmatched_headers = set(question_headers)
    used_item_ids = set()

    # 1) 정규화된 텍스트 기반 일치
    for item in registered_items:
        item_text = item.get("item_text", "")
        item_id = str(item.get("item_id", "") or "").strip()
        if not item_text or not item_id:
            continue

        item_norm = normalize_header_text(item_text)
        header = header_lookup.get(item_norm)
        if header and header not in header_to_item_id and item_id not in used_item_ids:
            header_to_item_id[header] = item_id
            used_item_ids.add(item_id)
            unmatched_headers.discard(header)

    # 2) 부분 매칭 (앞부분 비교)
    if unmatched_headers:
        for header in list(unmatched_headers):
            h_norm = normalize_header_text(header)
            for item in registered_items:
                item_text = item.get("item_text", "")
                item_id = str(item.get("item_id", "") or "").strip()
                if not item_text or not item_id or item_id in used_item_ids:
                    continue

                item_norm = normalize_header_text(item_text)
                if h_norm and item_norm and (h_norm in item_norm or item_norm in h_norm):
                    header_to_item_id[header] = item_id
                    used_item_ids.add(item_id)
                    unmatched_headers.discard(header)
                    break

    return header_to_item_id, sorted(unmatched_headers)


# ============================================================================
# 데이터 주입 메인 로직
# ============================================================================

def inject_responses_for_course(
    spreadsheet: gspread.Spreadsheet,
    course_id: str,
    file_path: str,
    description: str,
):
    """특정 Course의 응답 데이터 주입"""
    print(f"\n{'='*70}")
    print(f"📊 데이터 주입 시작: {description} (ID: {course_id})")
    print(f"{'='*70}")
    
    # 1. 파일 읽기
    print(f"\n1️⃣ 파일 읽기: {file_path}")
    try:
        df = read_response_file(file_path)
    except Exception as e:
        print(f"   ❌ 파일 읽기 실패: {str(e)}")
        return
    
    if df.empty:
        print(f"   ⚠️ 파일이 비어있습니다. 건너뜁니다.")
        return
    
    # 2. 헤더 분석
    print(f"\n2️⃣ 헤더 분석 ({len(df.columns)}개 컬럼)")
    headers = list(df.columns)

    question_headers = [h for h in headers if not is_pii_column(h)]
    pii_columns = [h for h in headers if is_pii_column(h)]

    if not question_headers:
        print("   ⚠️ 문항 열을 찾을 수 없습니다. 건너뜁니다.")
        return

    # Survey_Items 등록 및 Course 매핑 정리
    try:
        registered_items = ensure_survey_items_from_headers(spreadsheet, question_headers)
        removed_count = delete_course_item_mappings(spreadsheet, course_id)
        ensure_course_item_mapping(spreadsheet, course_id, registered_items)
        print(f"   ✅ Survey_Items 등록: {len(registered_items)}개 (기존 매핑 {removed_count}개 삭제 후 재생성)")
    except Exception as e:
        print(f"   ❌ Survey_Items/매핑 처리 실패: {str(e)}")
        return

    header_to_item_id, unmatched_headers = build_header_item_mapping(question_headers, registered_items)

    question_columns = []
    for header in question_headers:
        item_id = header_to_item_id.get(header)
        if item_id:
            question_columns.append((header, item_id))
        else:
            print(f"   ⚠️ 매핑 실패: '{header[:50]}...'")

    if unmatched_headers:
        print(f"   ⚠️ 매칭되지 않은 헤더: {len(unmatched_headers)}개")
        for header in unmatched_headers[:5]:
            print(f"      - {header[:70]}")
        if len(unmatched_headers) > 5:
            print(f"      ... 외 {len(unmatched_headers) - 5}개")

    print(f"   ✅ 문항 열: {len(question_columns)}개")
    print(f"   ✅ PII 열: {len(pii_columns)}개")
    
    # 3. 데이터 주입
    print(f"\n3️⃣ 데이터 주입 시작 ({len(df)} 응답자)")
    
    batch_id = generate_batch_id()
    injected_responses = 0
    injected_respondents = 0
    
    for idx, row in df.iterrows():
        try:
            # 응답자 ID 생성
            respondent_id = generate_respondent_id()
            
            # PII 추출 및 저장
            pii_data = extract_pii_from_row(row, PII_COLUMN_MAPPING)
            respondent_data = {
                "respondent_id": respondent_id,
                "course_id": course_id,
                "pii_consent": "",
                "company": pii_data.get("company", ""),
                "department": pii_data.get("department", ""),
                "job_role": pii_data.get("job_role", ""),
                "tenure_years": pii_data.get("tenure_years", ""),
                "name": pii_data.get("name", ""),
                "phone": pii_data.get("phone", ""),
                "email": pii_data.get("email", ""),
                "hashed_contact": "",
                "extra_meta": "",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            
            save_respondent(spreadsheet, respondent_data)
            injected_respondents += 1
            
            # 응답 데이터 저장
            for header, item_id in question_columns:
                answer_value = str(row[header]).strip() if pd.notna(row[header]) else ""
                
                if not answer_value or answer_value.lower() == "nan":
                    continue
                
                # 숫자 변환 시도
                response_value_num = None
                try:
                    response_value_num = float(answer_value)
                except (ValueError, TypeError):
                    pass
                
                response_data = {
                    "response_id": generate_response_id(),
                    "course_id": course_id,
                    "respondent_id": respondent_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "item_id": item_id,
                    "response_value": answer_value,
                    "response_value_num": response_value_num,
                    "choice_value": "",
                    "comment_text": answer_value,
                    "source_row_index": str(idx + 2),
                    "ingest_batch_id": batch_id,
                }
                
                save_response_v2(spreadsheet, response_data)
                injected_responses += 1
            
            # 진행 상황 표시
            if (idx + 1) % 10 == 0:
                print(f"   ⏳ 진행 중: {idx + 1}/{len(df)} 응답자 처리...")
                time.sleep(0.5)  # API 쿼터 보호
            
        except Exception as e:
            print(f"   ⚠️ 행 {idx + 2} 처리 실패: {str(e)}")
            continue
    
    print(f"\n   ✅ 주입 완료:")
    print(f"      - 응답자: {injected_respondents}명")
    print(f"      - 응답 데이터: {injected_responses}개")


# ============================================================================
# 메인 실행
# ============================================================================

def main():
    """메인 실행 함수"""
    print("\n" + "="*70)
    print("🚀 NCT 설문 응답 데이터 강제 주입 스크립트")
    print("="*70)
    
    # 1. Google Sheets 연결
    print("\n1️⃣ Google Sheets 연결 중...")
    try:
        client = get_client()
        spreadsheet = open_or_create_spreadsheet(client)
        print(f"   ✅ 연결 성공: {spreadsheet.title}")
    except Exception as e:
        print(f"   ❌ 연결 실패: {str(e)}")
        return
    
    # 2. 시트 클린징 (자동 실행)
    print("\n2️⃣ 시트 클린징 (기존 데이터 삭제)")
    print("   ⚠️ Responses 및 Respondents 시트의 모든 데이터를 삭제합니다...")
    print("   ✅ 자동 실행 모드로 진행합니다.")
    
    print("\n   🧹 시트 클린징 중...")
    clear_sheet_data(spreadsheet, "Responses")
    clear_sheet_data(spreadsheet, "Respondents")
    print("   ✅ 클린징 완료")
    
    # 3. 각 Course별 데이터 주입
    print("\n3️⃣ 응답 데이터 주입 시작")
    
    for mapping in COURSE_FILE_MAPPING:
        try:
            inject_responses_for_course(
                spreadsheet=spreadsheet,
                course_id=mapping["course_id"],
                file_path=mapping["file_path"],
                description=mapping["description"],
            )
            
            # Course 간 대기 (API 쿼터 보호)
            print("\n   ⏸️ 다음 Course 처리 전 3초 대기...")
            time.sleep(3)
            
        except Exception as e:
            print(f"\n   ❌ {mapping['description']} 주입 실패: {str(e)}")
            continue
    
    # 4. 완료
    print("\n" + "="*70)
    print("✅ 모든 데이터 주입 완료!")
    print("="*70)
    print("\n📋 다음 단계:")
    print("   1. Streamlit 앱으로 이동")
    print("   2. 관리자 모드 → DB 설정 → 캐시 클리어")
    print("   3. 관리자 모드 → 과정 리스트 → 결과 보기")
    print("   4. 분석 결과 확인 (📈 객관식, ⭐ 평점, 💬 주관식)\n")


if __name__ == "__main__":
    main()

