import os
import hashlib
import json
from datetime import datetime, date as datetime_date, timedelta, timezone
from typing import Dict, List
from collections import Counter, defaultdict
import io

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import time

from gsheets_utils import (
    get_client,
    open_or_create_spreadsheet,
    ensure_schema,
    upsert_course,
    list_courses,
    list_questions,
    upsert_question,
    delete_question,
    get_survey_settings,
    set_survey_active,
    save_response,
    update_response_stats,
    get_course_by_id,
    get_responses_for_course,
    get_responses_by_question,
    save_analysis,
    # 새 v2 스키마 함수들
    upsert_course_v2,
    list_courses_v2,
    get_course_by_id_v2,
    get_course_items,
    upsert_survey_item,
    map_item_to_course,
    save_response_v2,
    save_respondent,
    get_responses_v2,
    save_insight,
    get_insights,
    list_survey_items,
    get_survey_item_by_code,
    initialize_standard_lookups,
    initialize_standard_items,
    # 헤더 기반 자동 등록 함수들
    ensure_survey_items_from_headers,
    ensure_course_item_mapping,
    delete_course_item_mappings,
)


APP_TITLE = "교육 설문 플랫폼"
ADMIN_BADGE = "관리자 모드"


# ============================================================================
# 헬퍼 함수: ID 발급, 타입 추론 등
# ============================================================================

def generate_course_id() -> str:
    """course_id 자동 생성: C-YYYY-nnn 형식"""
    from datetime import datetime
    year = datetime.now().year
    random_suffix = str(int(datetime.utcnow().timestamp()))[-3:]
    return f"C-{year}-{random_suffix}"


def generate_item_id() -> str:
    """item_id 자동 생성"""
    return f"I-{int(datetime.utcnow().timestamp() * 1000)}"


def generate_respondent_id() -> str:
    """respondent_id 자동 생성"""
    import uuid
    return f"U-{str(uuid.uuid4())[:8]}"


def generate_response_id() -> str:
    """response_id 자동 생성"""
    return f"R-{int(datetime.utcnow().timestamp() * 1000000)}"


def generate_batch_id() -> str:
    """ingest_batch_id 생성"""
    return f"B-{int(datetime.utcnow().timestamp())}"


def normalize_company_name(company_name: str) -> str:
    """소속 회사명을 정규화하여 대소문자 및 일부 키워드 불일치를 해결
    
    Examples:
        "SK하이닉스" → "SKhynix"
        "주식회사 SK이노베이션" → "SKinnovation"
        "sk telecom" → "SKtelecom"
        "에스케이텔레콤" → "SKtelecom"
    """
    if not company_name or not str(company_name).strip():
        return ""
    
    # 1. 앞뒤 공백 제거 및 소문자 변환
    name = str(company_name).strip().lower()
    
    # 2. 불필요한 키워드/특수문자 제거
    replacements = {
        "주식회사": "", 
        "주)": "", 
        "(주)": "", 
        "㈜": "",
        " ": "",
        ".": "",
        ",": "",
        "하이닉스": "hynix", 
        "에스케이": "sk", 
        "이노베이션": "innovation",
        "텔레콤": "telecom"
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    
    # 3. 핵심 키워드 매핑 (가장 일반적인 SK 계열사)
    if "hynix" in name or "하이닉스" in name:
        return "SKhynix"
    if "innovation" in name or "이노베이션" in name:
        return "SKinnovation"
    if "telecom" in name or "텔레콤" in name:
        return "SKtelecom"
    if name in ["skt", "tsk"]:
        return "SKtelecom"
    
    # 4. 기타 SK 계열사 처리
    if name.startswith("sk") and len(name) > 2:
        # SK로 시작하는 경우 첫 글자를 대문자로
        return "SK" + name[2:].capitalize()
    
    # 5. 정규화 실패 시 원본 문자열을 타이틀 케이스로 반환
    return company_name.strip().title()


def infer_metric_type_from_text(text: str) -> str:
    """문항 텍스트에서 metric_type 추론"""
    text_lower = text.lower()

    # 🚨 핵심 수정: 메타데이터성 항목을 'text' 타입으로 강제 인식
    # "소속 회사", "직군", "연차", "회사명" 같은 항목은 주관식 텍스트로 수집
    metadata_text_keywords = ["직군", "연차", "회사명", "회사", "소속", "부서", "직무", "직책"]
    for keyword in metadata_text_keywords:
        if keyword in text_lower:
            return "text"

    if "만족" in text_lower or "평가" in text_lower or "점수" in text_lower:
        return "likert"
    elif "추천" in text_lower and ("10" in text or "0~10" in text):
        return "nps"
    elif "선택" in text_lower and ("하나" in text_lower or "단일" in text_lower):
        return "single_choice"
    elif "선택" in text_lower and ("여러" in text_lower or "복수" in text_lower or "다중" in text_lower):
        return "multi_choice"
    else:
        return "text"


def infer_dimension_from_text(text: str) -> str:
    """문항 텍스트에서 dimension 추론"""
    text_lower = text.lower()

    if "만족" in text_lower:
        return "satisfaction"
    elif "난이도" in text_lower or "어려" in text_lower:
        return "difficulty"
    elif "이해" in text_lower:
        return "understanding"
    elif "추천" in text_lower:
        return "recommend"
    elif "운영" in text_lower or "진행" in text_lower:
        return "operations"
    else:
        return "content"


def convert_answer_to_numeric(
    answer: str,
    metric_type: str,
     scale_max: int = 5) -> str:
    """응답을 숫자로 변환"""
    try:
        return str(float(answer))
    except (ValueError, TypeError):
        # 텍스트 매핑
        if metric_type in ["likert", "nps"]:
            if "매우" in str(answer) and (
    "만족" in str(answer) or "그렇다" in str(answer)):
                return str(scale_max)
            elif "만족" in str(answer) or "그렇다" in str(answer):
                return str(scale_max - 1)
            elif "보통" in str(answer):
                return str(scale_max // 2)
        return ""


def safe_str(val) -> str:
    """None/공백/특수문자를 안전하게 문자열로 변환"""
    if val is None:
        return ""

    # 문자열로 변환
    s = str(val)

    # Zero-width space 제거
    ZWSP = "\u200b"
    s = s.replace(ZWSP, "")

    # 앞뒤 공백 제거
    s = s.strip()

    # 연속된 공백/개행을 단일 공백으로
    import re
    s = re.sub(r'\s+', ' ', s)

    return s


def safe_date(val) -> str:
    """날짜를 안전하게 ISO 형식으로 변환"""
    if not val:
        return ""

    # 이미 date/datetime 객체인 경우
    if isinstance(val, (datetime_date, datetime)):
        return val.strftime("%Y-%m-%d") if hasattr(val,
                            'strftime') else str(val)

    # 문자열인 경우
    s = safe_str(val).replace("/", "-")
    if not s:
        return ""

    try:
        # YYYY-MM-DD 형식으로 파싱 시도
        parsed = datetime.strptime(s, "%Y-%m-%d")
        return parsed.date().isoformat()
    except Exception:
        # 파싱 실패 시 원본 반환
        return s


def read_uploaded_any(uploaded_file):
    """업로드된 파일을 안전하게 로드 (모든 시트 또는 CSV)
    
    Returns:
        (sheets_dict, meta) - sheets_dict는 Dict[str, DataFrame] 또는 None
    """
    if not uploaded_file:
        st.warning("📁 파일을 업로드해주세요.")
        return None, None

    filename = uploaded_file.name.lower()
    
    # 파일 포인터를 처음으로
    uploaded_file.seek(0)
    raw = uploaded_file.read()
    
    if not raw:
        st.error("❌ 업로드된 파일이 비어 있습니다.")
        return None, None

    buf = io.BytesIO(raw)
    meta = {"filename": filename, "size": len(raw)}

    try:
        if filename.endswith((".xlsx", ".xlsm")):
            # 엑셀 구조 건강검진 (Zip 유효성)
            import zipfile
            try:
                buf.seek(0)
                with zipfile.ZipFile(buf) as zf:
                    _ = zf.namelist()  # 접근만
            except zipfile.BadZipFile:
                st.error("❌ 엑셀 파일이 손상되었거나 압축 구조가 올바르지 않습니다.")
                st.info("💡 해결방법: 엑셀/구글시트에서 '다른 이름으로 저장' 후 다시 업로드해 주세요.")
                return None, meta

            buf.seek(0)
            # 모든 시트 로드: sheet_name=None → dict[str, DataFrame]
            try:
                xl = pd.read_excel(buf, sheet_name=None, engine="openpyxl", dtype=str)
                
                # 파일 포인터를 다시 처음으로
                uploaded_file.seek(0)
                
                return xl, meta
            except Exception as e:
                st.error(f"❌ Excel 파일 파싱 최종 실패: {uploaded_file.name}")
                st.warning("⚠️ 파일 내부 XML이 손상되었거나 호환되지 않는 형식입니다.")
                st.info(
                    "💡 **해결방법 (우선순위 순서)**:\n\n"
                    "1. **CSV 형식으로 변환** (가장 확실한 방법)\n"
                    "   - 엑셀에서 파일 열기 → '다른 이름으로 저장' → 'CSV UTF-8(쉼표로 분리)' 선택\n\n"
                    "2. **새 엑셀 파일로 재생성**\n"
                    "   - 파일 내용 전체 복사 → 새 Excel 파일에 붙여넣기 → 저장\n\n"
                    "3. **Google Sheets 경유**\n"
                    "   - Google Sheets에 업로드 → 다시 다운로드 (xlsx 또는 csv)"
                )
                with st.expander("🔍 상세 오류 메시지 (개발자 참고)"):
                    st.code(str(e))
                    st.caption("이 오류는 일반적으로 손상된 XML 구조, 지원되지 않는 Excel 기능 사용, 또는 파일 인코딩 문제로 발생합니다.")
                return None, meta

        elif filename.endswith(".xls"):
            buf.seek(0)
            try:
                # xlrd는 xls만 지원 (설치 필요)
                xl = pd.read_excel(buf, sheet_name=None, engine="xlrd", dtype=str)
                uploaded_file.seek(0)
                return xl, meta
            except ImportError:
                st.error("❌ .xls 파일 읽기를 위해 xlrd 패키지가 필요합니다.")
                st.info("💡 설치: pip install xlrd")
                return None, meta
            except Exception as e:
                st.error(f"❌ .xls 파일 파싱 실패: {str(e)}")
                return None, meta

        elif filename.endswith(".xlsb"):
            buf.seek(0)
            try:
                # pyxlsb 엔진 (설치 필요)
                xl = pd.read_excel(buf, sheet_name=None, engine="pyxlsb", dtype=str)
                uploaded_file.seek(0)
                return xl, meta
            except ImportError:
                st.error("❌ .xlsb 파일 읽기를 위해 pyxlsb 패키지가 필요합니다.")
                st.info("💡 설치: pip install pyxlsb")
                return None, meta
            except Exception as e:
                st.error(f"❌ .xlsb 파일 파싱 실패: {str(e)}")
                return None, meta

        elif filename.endswith(".csv"):
            # CSV는 단일 DF로 반환, 표준 인터페이스를 위해 dict로 감쌈
            df = None
            for encoding in ["utf-8-sig", "cp949", "euc-kr", "utf-8"]:
                try:
                    buf.seek(0)
                    df = pd.read_csv(buf, encoding=encoding, dtype=str)
                    break
                except UnicodeDecodeError:
                    continue
                except Exception:
                    continue
            
            if df is None:
                st.error("❌ CSV 인코딩 파싱 실패")
                st.info("💡 UTF-8 → CP949 → EUC-KR 순으로 시도했으나 모두 실패했습니다. CSV 인코딩을 확인하세요.")
                return None, meta
            
            uploaded_file.seek(0)
            return {"Questions": df}, meta

        else:
            st.error("❌ 지원하지 않는 파일 형식입니다.")
            st.info("💡 .xlsx / .xls / .xlsb / .csv 파일을 업로드해주세요.")
            return None, meta

    except Exception as e:
        st.error(f"❌ 파일 파싱 중 예상치 못한 오류 발생: {str(e)}")
        st.info("💡 파일이 손상되었거나 지원되지 않는 형식일 수 있습니다.")
        import traceback
        with st.expander("🔍 상세 오류 정보"):
            st.code(traceback.format_exc())
        return None, meta


def pick_questions_sheet(sheets_dict):
    """Questions 시트 선택/대체 로직
    
    Returns:
        DataFrame 또는 None
    """
    if sheets_dict is None or not sheets_dict:
        st.error("❌ 시트 데이터가 없습니다.")
        return None

    # 시트명 정규화
    keys = {k: k for k in sheets_dict.keys()}
    lower = {k.lower(): k for k in sheets_dict.keys()}
    
    # 디버깅: 시트 목록 표시
    with st.expander("📋 파일 내 시트 목록"):
        for idx, (name, df) in enumerate(sheets_dict.items(), 1):
            st.write(f"{idx}. **{name}** - {len(df)}행 × {len(df.columns)}열")

    # 1) 우선순위 매칭
    for candidate in ["questions", "문항", "질문", "survey_items", "설문문항", "sheet1"]:
        if candidate in lower:
            matched_key = lower[candidate]
            st.success(f"✅ '{matched_key}' 시트를 Questions로 자동 선택했습니다.")
            return sheets_dict[matched_key]

    # 2) 자동 추정: 컬럼 패턴 포함 DF 찾기
    def looks_like_questions(df):
        if df is None or df.empty:
            return False
        cols = [str(c).strip().lower() for c in df.columns]
        keywords = ["question", "문항", "질문", "옵션", "option", "scale", "응답", "answer", "choice"]
        hit = sum(any(k in c for k in keywords) for c in cols)
        return hit >= 1 and len(cols) >= 1 and len(df) >= 1

    candidates = [(name, df) for name, df in sheets_dict.items() 
                  if isinstance(df, pd.DataFrame) and looks_like_questions(df)]
    
    if len(candidates) == 1:
        name, df = candidates[0]
        st.success(f"✅ '{name}' 시트가 Questions 패턴과 일치하여 자동 선택했습니다.")
        return df

    # 3) 사용자가 선택하도록 드롭다운
    st.warning("⚠️ 'Questions' 시트를 자동으로 찾을 수 없습니다.")
    st.info("💡 아래에서 Questions 역할을 할 시트를 선택해 주세요.")
    
    choice = st.selectbox(
        "시트 선택",
        list(keys.keys()),
        format_func=lambda x: f"{x} ({len(sheets_dict[x])}행)"
    )
    
    if choice:
        return sheets_dict[keys[choice]]
    
    return None


def normalize_questions_wide(df):
    """wide 포맷 유효성 검사 & 정리
    
    Returns:
        정리된 DataFrame
    """
    if df is None or df.empty:
        st.error("❌ Questions 시트가 비어 있습니다.")
        return pd.DataFrame()
    
    # 공백/빈열 제거, 중복컬럼 처리
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated(keep="first")]
    
    # 완전 공백 행 제거
    df = df.dropna(how="all")
    
    if df.empty:
        st.error("❌ Questions 시트의 모든 행이 비어있거나 결측입니다.")
        return df
    
    return df


def load_uploaded_file(uploaded_file):
    """업로드된 파일을 자동 포맷 감지 후 DataFrame으로 로드 (레거시 호환)
    
    단일 DataFrame 반환 (기존 코드와 호환)
    """
    sheets, meta = read_uploaded_any(uploaded_file)
    if sheets is None:
        return None
    
    # Questions 시트 선택
    qdf = pick_questions_sheet(sheets)
    if qdf is None:
        st.error("❌ Questions 시트를 선택/추정하지 못했습니다.")
        return None
    
    # 정규화
    qdf = normalize_questions_wide(qdf)
    if qdf.empty:
        return None
    
    st.success(f"✅ Questions 시트 로드 완료: {meta['filename']} | {len(qdf)}행 × {len(qdf.columns)}열")
    
    with st.expander("👀 데이터 미리보기"):
        st.dataframe(qdf.head(10))
    
    return qdf


def set_page_config():
    st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")


def apply_global_styles():
    """Inject global CSS variables, fonts, and component theming for SK style."""
    # Plotly theme defaults (colors align with SK palette)
    try:
        primary = "#D90B31"
        secondary = "#404040"
        accent1 = "#F26680"
        accent2 = "#020659"
        neutral = "#D9D9D9"
        px.defaults.template = "plotly_white"
        px.defaults.color_discrete_sequence = [
    primary, accent1, accent2, secondary, neutral]
    except Exception:
        pass

    # Fonts: The Jamsil family (Noonnu CDN)
    st.markdown(
        """
        <style>
          @import url('https://cdn.jsdelivr.net/gh/projectnoonnu/noonfonts_2307@1.1.0/fonts/TheJamsil5Bold.woff2');
          @import url('https://cdn.jsdelivr.net/gh/projectnoonnu/noonfonts_2307@1.1.0/fonts/TheJamsil6ExtraBold.woff2');
          @font-face { font-family: 'TheJamsil-6'; src: url('https://cdn.jsdelivr.net/gh/projectnoonnu/noonfonts_2307@1.1.0/fonts/TheJamsil6ExtraBold.woff2') format('woff2'); font-weight: 800; font-style: normal; }
          @font-face { font-family: 'TheJamsil-5'; src: url('https://cdn.jsdelivr.net/gh/projectnoonnu/noonfonts_2307@1.1.0/fonts/TheJamsil5Bold.woff2') format('woff2'); font-weight: 700; font-style: normal; }
          @font-face { font-family: 'TheJamsil-4'; src: url('https://cdn.jsdelivr.net/gh/projectnoonnu/noonfonts_2307@1.1.0/fonts/TheJamsil4Medium.woff2') format('woff2'); font-weight: 500; font-style: normal; }
          @font-face { font-family: 'TheJamsil-3'; src: url('https://cdn.jsdelivr.net/gh/projectnoonnu/noonfonts_2307@1.1.0/fonts/TheJamsil3Regular.woff2') format('woff2'); font-weight: 400; font-style: normal; }

          :root {
            --pastel-blue: #A8D8EA;
            --pastel-purple: #D4A5D8;
            --pastel-pink: #FFB3C1;
            --pastel-mint: #B5EAD7;
            --pastel-lavender: #C7CEEA;
            --pastel-peach: #FFDAB9;
            --dark-text: #2C3E50;
            --light-text: #7F8C8D;
            --bg-soft: #F8F9FA;
          }

          html, body, [class^="main"] { font-family: 'TheJamsil-3', system-ui, -apple-system, Segoe UI, Roboto, 'Noto Sans KR', Arial, sans-serif; }

          /* SVG 아이콘 스타일 */
          .icon-svg {
            width: 24px;
            height: 24px;
            display: inline-block;
            vertical-align: middle;
            margin-right: 8px;
          }
          
          .icon-chip { stroke: var(--pastel-blue); fill: none; stroke-width: 2; }
          .icon-chart { stroke: var(--pastel-purple); fill: none; stroke-width: 2; }
          .icon-star { stroke: var(--pastel-peach); fill: none; stroke-width: 2; }
          .icon-message { stroke: var(--pastel-mint); fill: none; stroke-width: 2; }
          .icon-brain { stroke: var(--pastel-lavender); fill: none; stroke-width: 2; }

          /* Headings */
          h1, .sk-h1 { 
            font-family: 'TheJamsil-6', sans-serif; 
            color: var(--dark-text); 
            letter-spacing: -0.2px; 
          }
          h2, .sk-h2 { 
            font-family: 'TheJamsil-5', sans-serif; 
            color: var(--dark-text); 
          }
          h3, .sk-h3 { 
            font-family: 'TheJamsil-5', sans-serif; 
            color: var(--dark-text); 
          }

          /* Sidebar */
          section[data-testid="stSidebar"] { 
            border-right: 2px solid var(--pastel-lavender);
            background: linear-gradient(180deg, #ffffff 0%, var(--bg-soft) 100%);
          }
          section[data-testid="stSidebar"] .css-1d391kg, 
          section[data-testid="stSidebar"] * { 
            font-family: 'TheJamsil-4', sans-serif; 
          }

          /* Buttons */
          .stButton > button {
            background: linear-gradient(135deg, var(--pastel-blue) 0%, var(--pastel-lavender) 100%) !important;
            color: var(--dark-text) !important;
            border: 2px solid var(--pastel-purple) !important;
            border-radius: 12px !important;
            padding: 0.6rem 1.2rem !important;
            font-family: 'TheJamsil-5', sans-serif;
            box-shadow: 0 4px 12px rgba(168, 216, 234, 0.3);
            transition: all 0.3s ease;
          }
          .stButton > button:hover { 
            transform: translateY(-2px);
            box-shadow: 0 6px 16px rgba(168, 216, 234, 0.5);
          }
          .stButton > button:focus { 
            outline: 3px solid var(--pastel-mint);
            outline-offset: 2px;
          }

          /* Selects, inputs */
          .stSelectbox div[data-baseweb="select"] > div,
          .stTextInput > div > div > input,
          .stTextArea textarea,
          .stRadio,
          .stSlider {
            font-family: 'TheJamsil-3', sans-serif !important;
          }

          /* Tabs */
          button[role="tab"] { 
            font-family: 'TheJamsil-4', sans-serif;
            color: var(--light-text);
            transition: all 0.3s ease;
          }
          button[role="tab"][aria-selected="true"] { 
            color: var(--dark-text);
            border-bottom: 3px solid var(--pastel-blue);
            background: linear-gradient(180deg, transparent 0%, rgba(168, 216, 234, 0.1) 100%);
          }
          button[role="tab"]:hover {
            color: var(--dark-text);
            background: rgba(168, 216, 234, 0.05);
          }

          /* 파스텔 배경 장식 */
          .ai-bg-decoration {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            z-index: -1;
            opacity: 0.4;
            background-image: 
              radial-gradient(circle at 20% 30%, rgba(168, 216, 234, 0.3) 0%, transparent 50%),
              radial-gradient(circle at 80% 70%, rgba(212, 165, 216, 0.2) 0%, transparent 50%),
              radial-gradient(circle at 50% 50%, rgba(181, 234, 215, 0.15) 0%, transparent 60%);
          }

          /* Cards - 파스텔 선형 스타일 */
          .sk-card {
            border: 2px solid var(--pastel-lavender);
            border-radius: 20px;
            padding: 24px;
            background: linear-gradient(145deg, #ffffff 0%, rgba(168, 216, 234, 0.05) 100%);
            box-shadow: 
              0 8px 24px rgba(168, 216, 234, 0.15),
              0 4px 12px rgba(212, 165, 216, 0.1),
              inset 0 1px 0 rgba(255, 255, 255, 0.9);
            position: relative;
            overflow: hidden;
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
          }
          
          .sk-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, 
              var(--pastel-blue) 0%, 
              var(--pastel-purple) 25%, 
              var(--pastel-pink) 50%, 
              var(--pastel-mint) 75%, 
              var(--pastel-lavender) 100%);
            border-radius: 20px 20px 0 0;
            opacity: 0.7;
            transition: all 0.4s ease;
          }
          
          .sk-card:hover {
            transform: translateY(-6px);
            border-color: var(--pastel-purple);
            box-shadow: 
              0 12px 32px rgba(168, 216, 234, 0.25),
              0 6px 16px rgba(212, 165, 216, 0.2),
              inset 0 1px 0 rgba(255, 255, 255, 1);
          }
          
          .sk-card:hover::before {
            height: 4px;
            opacity: 1;
          }
          
          .sk-card h4 {
            margin: 0 0 12px 0;
            font-family: 'TheJamsil-5';
            color: var(--dark-text);
            font-size: 1.2rem;
            display: flex;
            align-items: center;
          }
          
          .sk-card .sk-desc {
            color: var(--light-text);
            font-family: 'TheJamsil-3';
            line-height: 1.7;
          }
          
          /* 선형 패턴 장식 */
          .sk-card::after {
            content: '';
            position: absolute;
            bottom: 0;
            right: 0;
            width: 120px;
            height: 120px;
            background-image: 
              repeating-linear-gradient(45deg, transparent, transparent 10px, rgba(168, 216, 234, 0.03) 10px, rgba(168, 216, 234, 0.03) 20px),
              repeating-linear-gradient(-45deg, transparent, transparent 10px, rgba(212, 165, 216, 0.03) 10px, rgba(212, 165, 216, 0.03) 20px);
            opacity: 0.6;
            pointer-events: none;
            border-radius: 0 0 20px 0;
          }

          /* Page header band */
          .sk-page-header {
            padding: 20px 24px;
            border-left: 5px solid var(--pastel-blue);
            background: linear-gradient(135deg, #ffffff 0%, rgba(168, 216, 234, 0.08) 100%);
            border-radius: 16px;
            box-shadow: 
              0 4px 16px rgba(168, 216, 234, 0.15),
              0 2px 8px rgba(212, 165, 216, 0.1);
            position: relative;
            border: 2px solid var(--pastel-lavender);
            border-left: 5px solid var(--pastel-blue);
          }
          
          .sk-page-header .title {
            font-family: 'TheJamsil-6';
            color: var(--dark-text);
            font-size: 1.7rem;
            margin-bottom: 4px;
          }
          
          .sk-page-header .subtitle {
            color: var(--light-text);
            font-size: 1rem;
          }

          /* KPIs */
          [data-testid="stMetricValue"] {
            color: var(--dark-text);
            font-family: 'TheJamsil-6';
            font-size: 1.8rem !important;
          }
          
          [data-testid="stMetric"] {
            background: linear-gradient(135deg, #ffffff 0%, rgba(168, 216, 234, 0.05) 100%);
            border-radius: 16px;
            padding: 16px;
            border: 2px solid var(--pastel-lavender);
            box-shadow: 0 4px 12px rgba(168, 216, 234, 0.1);
            transition: all 0.3s ease;
          }
          
          [data-testid="stMetric"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 16px rgba(168, 216, 234, 0.2);
            border-color: var(--pastel-blue);
          }
          
          [data-testid="stMetricLabel"] {
            color: var(--light-text);
            font-family: 'TheJamsil-4';
          }

          /* Mobile adjustments */
          @media (max-width: 480px) {
            .sk-page-header .title { font-size: 1.25rem; }
            .stButton > button { width: 100%; }
            .stRadio label, .stSelectbox { font-size: 0.95rem; }
            .stTextArea textarea { min-height: 120px; }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_admin_password() -> str:
    # Priority: env -> st.secrets
    pwd = os.getenv("SURVEY_ADMIN_PASSWORD")
    if not pwd and hasattr(
    st,
     "secrets") and "SURVEY_ADMIN_PASSWORD" in st.secrets:
        pwd = st.secrets["SURVEY_ADMIN_PASSWORD"]
    return pwd or "skms2024"  # fallback for local dev


@st.cache_resource(ttl=600)  # Cache for 10 minutes to reduce API calls
def require_spreadsheet():
    """Get spreadsheet with caching to avoid quota issues"""
    import time

    client = get_client()
    # Use fixed spreadsheet ID from env/secrets or fallback to provided ID
    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not sheet_id and hasattr(
    st, "secrets") and "GOOGLE_SHEETS_SPREADSHEET_ID" in st.secrets:
        sheet_id = st.secrets["GOOGLE_SHEETS_SPREADSHEET_ID"]
    if not sheet_id:
        # Fallback to the provided Sheet ID
        sheet_id = "1sxwBgqSqxHw1mqfxAHskspO-SCpEDWTAioII_pp7hHs"

    # Retry logic for quota errors with exponential backoff
    max_retries = 5
    for attempt in range(max_retries):
        try:
            spreadsheet = open_or_create_spreadsheet(
                client, spreadsheet_id=sheet_id)
            ensure_schema(spreadsheet)
            return spreadsheet
        except Exception as e:
            if "429" in str(e) or "Quota exceeded" in str(
                e) or "quota" in str(e).lower():
                if attempt < max_retries - 1:
                    # 2, 4, 8, 16, 32 seconds (exponential)
                    wait_time = (2 ** attempt) * 2
                    st.warning(
    f"⏳ Google Sheets API 쿼터 제한 감지. {wait_time}초 후 재시도합니다... (시도 {
        attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    st.error("⚠️ Google Sheets API 쿼터가 계속 초과되고 있습니다.")
                    st.info(
                        "💡 해결 방법:\n- 페이지를 2-3분 후에 새로고침하세요.\n- 여러 사용자가 동시에 접근 중이라면 잠시 대기하세요.\n- API 쿼터가 부족하면 Google Cloud Console에서 쿼터 증가를 요청하세요.")
                    st.stop()
            else:
                st.error(f"오류: {str(e)}")
                st.stop()


def sidebar_mode_selector():
    """사이드바에 모드 선택기 표시"""
    st.sidebar.markdown(
        """
        <div class="sk-page-header" style="margin-bottom:8px;">
          <div class="title" style="font-size:1.1rem;">🔧 모드 선택</div>
          <div class="subtitle" style="font-size:0.9rem;">사용자 / 관리자</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    mode = st.sidebar.selectbox(
        "접근 모드를 선택하세요:",
        ["일반 사용자", "관리자"],
        index=0
    )
    return mode == "관리자"


def authenticate_if_needed(is_admin_mode: bool):
    if is_admin_mode and not st.session_state.get("admin_authenticated"):
        with st.sidebar:
            st.markdown("### 관리자 인증")
            pwd = st.text_input("비밀번호", type="password")
            if st.button("인증"):
                if pwd == get_admin_password():
                    st.session_state["admin_authenticated"] = True
                    st.session_state["admin_expire_at"] = (
                        datetime.now(timezone.utc) + timedelta(minutes=30)
                    ).isoformat()
                    st.success("인증 성공")
                else:
                    st.error("비밀번호가 올바르지 않습니다")

    # Expire admin session
    expire_at = st.session_state.get("admin_expire_at")
    if expire_at and datetime.now(
    timezone.utc) > datetime.fromisoformat(expire_at):
        st.session_state["admin_authenticated"] = False
        st.session_state["admin_expire_at"] = None


def page_setup_db(spreadsheet):
    st.subheader("데이터베이스 초기화")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📋 스키마 생성")
        st.write("필요한 시트를 생성하고 헤더를 설정합니다.")
        if st.button("스키마 보증 실행"):
            with st.spinner("스키마 생성 중..."):
                ensure_schema(spreadsheet)
                st.success("✅ 시트 스키마가 준비되었습니다.")

    with col2:
        st.markdown("#### ✨ v2 스키마 초기화")
        st.write("표준값 및 표준 문항을 초기화합니다.")
        if st.button("v2 초기화 실행"):
            with st.spinner("v2 스키마 초기화 중..."):
                try:
                    # 1. 표준값 사전 초기화
                    initialize_standard_lookups(spreadsheet)
                    st.success("✅ Lookups 시트 초기화 완료")

                    # 2. 표준 문항 초기화
                    initialize_standard_items(spreadsheet)
                    st.success("✅ Survey_Items 시트 초기화 완료")

                    st.balloons()
                    st.info("💡 이제 표준 문항을 재사용하여 새 과정을 만들 수 있습니다!")
                except Exception as e:
                    st.error(f"초기화 실패: {str(e)}")

    # 시트 목록 표시
    st.divider()
    st.markdown("#### 📊 현재 시트 목록")

    try:
        worksheets = spreadsheet.worksheets()

        # 새 스키마 시트
        v2_sheets = [
    "Survey_Items",
    "Course_Item_Map",
    "Respondents",
    "Insights",
     "Lookups"]
        # 레거시 시트
        legacy_sheets = ["Questions", "ResponseStats", "Analysis"]

        cols = st.columns(3)
        for idx, ws in enumerate(worksheets):
            with cols[idx % 3]:
                if ws.title in v2_sheets:
                    st.success(f"✨ {ws.title} (v2)")
                elif ws.title in legacy_sheets:
                    st.warning(f"⚠️ {ws.title} (레거시)")
                else:
                    st.info(f"📄 {ws.title}")

        # 레거시 시트 정리 안내
        if any(ws.title in legacy_sheets for ws in worksheets):
            st.divider()
            st.warning("⚠️ 레거시 시트가 발견되었습니다.")
            st.caption("터미널에서 다음 명령으로 정리할 수 있습니다:")
            st.code(
    "python cleanup_legacy_sheets.py --dry-run",
     language="bash")

    except Exception as e:
        st.error(f"시트 목록 조회 실패: {str(e)}")


def _detect_uploaded_frames(uploaded_file) -> Dict[str, pd.DataFrame]:
    """업로드된 파일에서 Course/Questions/Responses를 자동 감지해 DataFrame으로 반환"""
    dfs: Dict[str, pd.DataFrame] = {}
    try:
        if uploaded_file.name.lower().endswith(".xlsx"):
            xls = pd.ExcelFile(uploaded_file, engine='openpyxl')
            sheet_names_lower = {s.lower(): s for s in xls.sheet_names}
            # 표준 시트명 우선
            if "course" in sheet_names_lower:
                dfs["course"] = pd.read_excel(
    xls, sheet_name=sheet_names_lower["course"], engine='openpyxl')  # type: ignore
            if "questions" in sheet_names_lower:
                dfs["questions"] = pd.read_excel(
    xls, sheet_name=sheet_names_lower["questions"], engine='openpyxl')  # type: ignore
            if "responses" in sheet_names_lower:
                dfs["responses"] = pd.read_excel(
    xls, sheet_name=sheet_names_lower["responses"], engine='openpyxl')  # type: ignore
            # 보조: 첫 1~3 시트를 heuristic으로 매핑
            if not dfs:
                sheets = xls.sheet_names[:3]
                for s in sheets:
                    df = pd.read_excel(xls, sheet_name=s, engine='openpyxl')
                    cols = {c.strip().lower() for c in df.columns.astype(str)}
                    if {"courseid", "title"}.issubset(cols):
                        dfs["course"] = df
                    elif {"questionid", "text", "type"}.issubset(cols):
                        dfs["questions"] = df
                    elif {"courseid", "questionid", "answer"}.issubset(cols):
                        dfs["responses"] = df
        else:
            # CSV: 헤더 기반으로 유형 감지 (다중 인코딩 시도)
            data = uploaded_file.read()
            buf = io.BytesIO(data)
            df = None
            for enc in (
    None,
    "utf-8",
    "utf-8-sig",
    "cp949",
    "euc-kr",
     "latin1"):
                try:
                    buf.seek(0)
                    if enc is None:
                        df = pd.read_csv(buf)
                    else:
                        df = pd.read_csv(buf, encoding=enc)
                    break
                except Exception:
                    continue
            if df is None:
                raise ValueError("CSV 인코딩을 판별하지 못했습니다.")
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            cols = {c.strip().lower() for c in df.columns.astype(str)}
            if {"courseid", "title"}.issubset(cols):
                dfs["course"] = df
            if {"questionid", "text", "type"}.issubset(cols):
                dfs["questions"] = df
            if {"courseid", "questionid", "answer"}.issubset(cols):
                dfs["responses"] = df
    except Exception as e:
        st.error(f"파일 파싱 실패: {str(e)}")
    return dfs


def _normalize_question_row(row: Dict) -> Dict:
    """업로드 질문 행을 내부 스키마로 정규화"""
    def get_str(key: str, default: str = ""):
        v = row.get(key)
        return "" if v is None else str(v).strip()

    q: Dict[str, str] = {}
    q["questionId"] = get_str("questionId") or get_str(
        "id") or str(int(datetime.utcnow().timestamp() * 1000))
    q["courseId"] = get_str("courseId")
    q["order"] = get_str("order") or get_str("displayOrder") or ""
    q["text"] = get_str("text") or get_str("question")
    q_type = (get_str("type") or "subjective").lower()
    if q_type not in {"objective", "subjective", "rating"}:
        q_type = "subjective"
    q["type"] = q_type

    # choices
    choices_json = get_str("choicesJson")
    if not choices_json and get_str("choices"):
        raw = [c.strip() for c in get_str("choices").split(",") if c.strip()]
        choices_json = "[" + \
            ",".join([f'\"{c}\"' for c in raw]) + "]" if raw else "[]"
    q["choicesJson"] = choices_json or "[]"

    # rating
    q["ratingMax"] = get_str("ratingMax") or (get_str("maxRating") or "")

    # required
    is_required = get_str("isRequired") or get_str("required")
    q["isRequired"] = "TRUE" if is_required.lower() in {
    "true", "1", "yes", "y"} else (
        "TRUE" if is_required == "TRUE" else "FALSE")

    # max chars
    q["maxChars"] = get_str("maxChars") or get_str("maxLength") or "0"
    return q


def _normalize_course_row(row: Dict) -> Dict:
    def gs(key: str, default: str = ""):
        v = row.get(key)
        return default if v is None else str(v).strip() or default

    return {
        "courseId": gs("courseId") or gs("id", ""),
        "title": gs("title", "(제목없음)"),
        "description": gs("description", ""),
        "category": gs("category", "기본"),
        "createdAt": gs("createdAt", datetime.utcnow().isoformat()),
        "status": (gs("status", "active").lower() if gs("status", "active") else "active"),
        "ownerId": gs("ownerId", "admin"),
    }


def _normalize_response_row(row: Dict) -> Dict:
    def gs(key: str, default: str = ""):
        v = row.get(key)
        return default if v is None else str(v)

    return {
        "courseId": gs("courseId"),
        "questionId": gs("questionId"),
        "answer": gs("answer"),
        "respondentHash": gs("respondentHash", "import" + hashlib.md5(json.dumps(row, ensure_ascii=False).encode()).hexdigest()[:8]),
        "sessionId": gs("sessionId", "import_session"),
        "ipMasked": gs("ipMasked", "***.***.***.***"),
        "timestamp": gs("timestamp", datetime.utcnow().isoformat()),
    }


def _is_metadata_column(column_text: str) -> bool:
    """메타데이터/PII 열인지 판단 (설문 문항이 아닌 응답자 정보)"""
    column_lower = column_text.lower()
    
    # 🚨 핵심 수정: 회사/소속/부서/직군 등은 설문 문항으로 포함
    # 메타데이터이지만 분석 가치가 있으므로 문항으로 등록
    # PII(개인식별정보)만 제외
    pii_keywords = [
        "이름", "성함", "성명", "name",
        "연락처", "전화", "휴대폰", "핸드폰", "phone", "mobile", "tel",
        "이메일", "메일", "email", "e-mail",
        "경품", "동의", "개인정보", "prize", "consent", "privacy",
        "주소", "address",
        "생년월일", "birthday", "birth",
    ]
    
    # 키워드가 포함되어 있으면 PII로 간주
    for keyword in pii_keywords:
        if keyword in column_lower:
            return True
    
    return False


def _parse_wide_excel_first_sheet(uploaded_file) -> Dict[str, List[Dict]]:
    """Parse an Excel where row1 columns are questions and row2+ are responses.
    
    🔧 안정성 강화: 개별 셀 오류를 건너뛰고 최대한 많은 데이터를 파싱합니다.

    Returns dict with keys:
      - questions: List[Dict]
      - responses: List[Dict] each has questionId, answer, respondentIndex
      - skipped_columns: List[str] (메타데이터로 건너뛴 열 목록)
    """
    result: Dict[str, List[Dict]] = {"questions": [], "responses": [], "skipped_columns": []}
    try:
        # Read into buffer to avoid consuming original pointer irreversibly
        data = uploaded_file.read()
        buf = io.BytesIO(data)
        xls = pd.ExcelFile(buf, engine='openpyxl')
        sheet_name = xls.sheet_names[0]
        
        # 🔧 dtype=str로 모든 데이터를 문자열로 읽어 형식 오류 방지
        df = pd.read_excel(xls, sheet_name=sheet_name, header=None, engine='openpyxl', dtype=str)
        if df.empty:
            return result

        # First row -> question texts (skip first column: timestamp)
        header_row = []
        try:
            header_row = df.iloc[0].fillna("").astype(str).tolist()
        except Exception as e:
            st.warning(f"⚠️ 헤더 행 파싱 오류: {str(e)}")
            return result
        
        # Data rows -> responses
        data_df = df.iloc[1:].reset_index(drop=True)

        # Build questions (메타데이터 열 제외)
        questions: List[Dict] = []
        col_to_qid: Dict[int, str] = {}
        skipped_columns: List[str] = []
        
        for idx, q_text in enumerate(header_row):
            try:
                # 첫 번째 열(타임스탬프) 건너뛰기
                if idx == 0:
                    continue
                
                # 빈 열 건너뛰기
                if not str(q_text).strip():
                    continue
                
                # 🚨 메타데이터/PII 열 감지 및 건너뛰기
                if _is_metadata_column(str(q_text)):
                    skipped_columns.append(f"[열 {idx+1}] {str(q_text)[:50]}")
                    continue
                
                # 문항으로 등록
                qid = str(int(datetime.utcnow().timestamp() * 1000)) + f"{idx:02d}"
                col_to_qid[idx] = qid
                questions.append({
                    "questionId": qid,
                    "order": str(len(questions) + 1),
                    "text": str(q_text).strip(),
                    "type": "subjective",
                    "choicesJson": "[]",
                    "ratingMax": "",
                    "isRequired": "FALSE",
                    "maxChars": "0",
                })
            except Exception as e:
                # 개별 열 파싱 오류는 건너뛰고 계속 진행
                st.warning(f"⚠️ 열 {idx+1} 파싱 오류 (건너뜀): {str(e)}")
                continue

        # Build responses (개별 셀 오류 처리)
        responses: List[Dict] = []
        for ridx in range(len(data_df)):
            try:
                row_series = data_df.iloc[ridx]
                for cidx, val in enumerate(row_series.tolist()):
                    try:
                        if cidx not in col_to_qid:
                            continue
                        # 🔧 안전한 문자열 변환
                        ans = "" if pd.isna(val) or val is None else str(val).strip()
                        responses.append({
                            "questionId": col_to_qid[cidx],
                            "answer": ans,
                            "respondentIndex": ridx,
                        })
                    except Exception as cell_err:
                        # 개별 셀 오류는 건너뛰고 계속
                        continue
            except Exception as row_err:
                # 행 전체 오류는 로그만 남기고 계속
                st.warning(f"⚠️ 행 {ridx+2} 파싱 오류 (건너뜀): {str(row_err)}")
                continue

        result["questions"] = questions
        result["responses"] = responses
        result["skipped_columns"] = skipped_columns
        
        # 파싱 결과 요약
        if len(questions) > 0:
            st.success(f"✅ 엑셀 파싱 성공: {len(questions)}개 문항, {len(responses)}개 응답")
        
    except Exception as e:
        error_msg = str(e)
        
        # XML 관련 오류 특별 처리
        if "XML" in error_msg or "manifest" in error_msg or "openpyxl" in error_msg:
            st.error("❌ 엑셀 파일 XML 구조 오류: 파일이 손상되었거나 지원되지 않는 형식입니다.")
            st.warning("⚠️ 이 오류는 일반적으로 다음과 같은 경우 발생합니다:")
            st.write("   1. Google Forms에서 직접 다운로드한 XLSX 파일")
            st.write("   2. 온라인 도구로 변환된 XLSX 파일")
            st.write("   3. 손상된 엑셀 파일")
            st.error("🚨 **필수**: Excel에서 파일을 열고 CSV UTF-8로 다시 저장해야 합니다!")
            with st.expander("📝 CSV 변환 방법 (상세)"):
                st.markdown("""
                ### Excel에서 CSV로 변환하는 방법:
                
                1. **엑셀에서 파일 열기** (.xlsx 파일)
                2. `파일` → `다른 이름으로 저장` 클릭
                3. `파일 형식` 드롭다운에서 선택:
                   - **"CSV UTF-8 (쉼표로 분리)(*.csv)"** ← 이것 선택!
                4. 파일명 확인 후 `저장` 클릭
                5. 경고 메시지 나오면 `예` 클릭
                6. 저장된 .csv 파일을 업로드
                
                ⚠️ 주의: "CSV (쉼표로 분리)" 가 아니라 **"CSV UTF-8"** 을 선택하세요!
                """)
        else:
            st.error(f"❌ wide 포맷 파싱 실패: {error_msg}")
            st.info("💡 파일을 **CSV 형식**으로 변환하여 재업로드를 권장합니다.")
    finally:
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
    return result


def _parse_wide_csv(uploaded_file) -> Dict[str, List[Dict]]:
    """Parse a CSV where col1 is timestamp, row1 columns are questions (from col2), and row2+ are responses.
    
    🔧 안정성 강화: 개별 셀 오류를 건너뛰고 최대한 많은 데이터를 파싱합니다.

    Returns dict with keys:
      - questions: List[Dict]
      - responses: List[Dict] each has questionId, answer, respondentIndex
      - skipped_columns: List[str] (메타데이터로 건너뛴 열 목록)
    """
    result: Dict[str, List[Dict]] = {"questions": [], "responses": [], "skipped_columns": []}
    try:
        data = uploaded_file.read()
        buf = io.BytesIO(data)
        
        # 🚨 핵심 수정: 인코딩 자동 감지 로직 강화 및 상세 오류 메시지
        df = None
        encoding_used = None
        encoding_errors = []
        
        # utf-8-sig를 먼저 시도해야 BOM(Byte Order Mark) 문제 해결
        for encoding in ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8', 'latin-1']:
            try:
                buf.seek(0)
                # 🔧 dtype=str로 모든 데이터를 문자열로 읽어 형식 오류 방지
                df = pd.read_csv(buf, header=None, encoding=encoding, dtype=str, on_bad_lines='skip')
                encoding_used = encoding
                st.success(f"✅ CSV 인코딩 감지 성공: {encoding_used}")
                break
            except Exception as e:
                encoding_errors.append(f"{encoding}: {str(e)[:50]}")
                continue
        
        if df is None or df.empty:
            st.error("❌ CSV 인코딩 파싱 실패: 파일 인코딩을 (UTF-8 with BOM 또는 CP949)로 저장 후 재업로드하십시오.")
            with st.expander("🔍 인코딩 시도 내역"):
                for err in encoding_errors:
                    st.write(f"  - {err}")
            st.info("💡 Excel에서 CSV 저장 시: '다른 이름으로 저장' → 'CSV UTF-8 (쉼표로 분리)(*.csv)' 선택")
            return result

        # First row -> question texts (skip first column: timestamp)
        header_row = []
        try:
            header_row = df.iloc[0].fillna("").astype(str).tolist()
        except Exception as e:
            st.warning(f"⚠️ CSV 헤더 행 파싱 오류: {str(e)}")
            return result
        
        # Data rows -> responses
        data_df = df.iloc[1:].reset_index(drop=True)

        questions: List[Dict] = []
        col_to_qid: Dict[int, str] = {}
        skipped_columns: List[str] = []
        
        for idx in range(1, len(header_row)):
            try:
                q_text = header_row[idx]
                
                # 빈 열 건너뛰기
                if not str(q_text).strip():
                    continue
                
                # 🚨 메타데이터/PII 열 감지 및 건너뛰기
                if _is_metadata_column(str(q_text)):
                    skipped_columns.append(f"[열 {idx+1}] {str(q_text)[:50]}")
                    continue
                
                # 문항으로 등록
                qid = str(int(datetime.utcnow().timestamp() * 1000)) + f"{idx:02d}"
                col_to_qid[idx] = qid
                questions.append({
                    "questionId": qid,
                    "order": str(len(questions) + 1),
                    "text": str(q_text).strip(),
                    "type": "subjective",
                    "choicesJson": "[]",
                    "ratingMax": "",
                    "isRequired": "FALSE",
                    "maxChars": "0",
                })
            except Exception as e:
                # 개별 열 파싱 오류는 건너뛰고 계속
                st.warning(f"⚠️ CSV 열 {idx+1} 파싱 오류 (건너뜀): {str(e)}")
                continue

        # Build responses (개별 셀 오류 처리)
        responses: List[Dict] = []
        for ridx in range(len(data_df)):
            try:
                row_series = data_df.iloc[ridx]
                for cidx in range(1, len(row_series)):
                    try:
                        if cidx not in col_to_qid:
                            continue
                        val = row_series.iloc[cidx]
                        # 🔧 안전한 문자열 변환
                        ans = "" if pd.isna(val) or val is None else str(val).strip()
                        responses.append({
                            "questionId": col_to_qid[cidx],
                            "answer": ans,
                            "respondentIndex": ridx,
                        })
                    except Exception as cell_err:
                        # 개별 셀 오류는 건너뛰고 계속
                        continue
            except Exception as row_err:
                # 행 전체 오류는 로그만 남기고 계속
                st.warning(f"⚠️ CSV 행 {ridx+2} 파싱 오류 (건너뜀): {str(row_err)}")
                continue

        result["questions"] = questions
        result["responses"] = responses
        result["skipped_columns"] = skipped_columns
        
        # 파싱 결과 요약
        if len(questions) > 0:
            st.success(f"✅ CSV 파싱 성공 ({encoding_used}): {len(questions)}개 문항, {len(responses)}개 응답")
        
    except Exception as e:
        st.error(f"❌ CSV wide 포맷 파싱 실패: {str(e)}")
        st.info("💡 파일을 다시 저장하거나 다른 인코딩(UTF-8)으로 저장해보세요.")
    finally:
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
    return result


def page_upload_files(spreadsheet):
    """관리자: 설문 파일 업로드 (문항만 또는 응답 포함)"""
    st.subheader("설문 파일 업로드 (CSV/XLSX)")
    st.caption(
        "- XLSX는 'Course', 'Questions', 'Responses' 시트명을 지원합니다.\n- CSV는 헤더로 유형을 자동 감지합니다.")

    # 세션 상태 초기화 (course_id 덮어쓰기 방지)
    if 'upload_course_id' not in st.session_state:
        st.session_state.upload_course_id = ""
    if 'course_id_user_edited' not in st.session_state:
        st.session_state.course_id_user_edited = False

    uploaded = st.file_uploader(
    "파일을 업로드하세요",
    type=[
        "xlsx",
        "csv"],
         accept_multiple_files=False)

    if not uploaded:
        # 파일 없으면 세션 초기화
        st.session_state.upload_course_id = ""
        st.session_state.course_id_user_edited = False
        return

    # 파일이 업로드되고, 기본값이 없고, 사용자가 편집하지 않았으면 초기값 생성 (1회만)
    if not st.session_state.upload_course_id and not st.session_state.course_id_user_edited:
        st.session_state.upload_course_id = generate_course_id()

    # Wide-matrix option (row1=questions, row2+=responses)
    use_wide_format = st.checkbox("1행=설문 문항, 2행부터=응답 (와이드 포맷)")
    
    # CSV 우선 권장 메시지
    if uploaded and uploaded.name.lower().endswith(".xlsx"):
        st.warning("⚠️ XLSX 파일은 XML 오류가 발생할 수 있습니다. CSV UTF-8 형식을 권장합니다!")
        with st.expander("💡 빠른 해결 방법"):
            st.markdown("""
            **Excel에서 CSV로 변환:**
            1. 현재 파일을 Excel에서 열기
            2. `파일` → `다른 이름으로 저장`
            3. `CSV UTF-8 (쉼표로 분리)(*.csv)` 선택
            4. 저장 후 CSV 파일 업로드
            """)

    # 와이드 포맷(CSV/XLSX) 선택 시, 사전 감지를 건너뛰어 인코딩/포인터 이슈 회피
    if use_wide_format and (uploaded.name.lower().endswith(
        ".csv") or uploaded.name.lower().endswith(".xlsx")):
        dfs = {}
    else:
        dfs = _detect_uploaded_frames(uploaded)
    has_course = "course" in dfs and not dfs["course"].empty
    has_questions = "questions" in dfs and not dfs["questions"].empty
    has_responses = "responses" in dfs and not dfs["responses"].empty

    with st.expander("미리보기"):
        if use_wide_format and (uploaded.name.lower().endswith(
            ".csv") or uploaded.name.lower().endswith(".xlsx")):
            st.markdown("**와이드 포맷 감지**: 1행 문항, 2행부터 응답")
            try:
                if uploaded.name.lower().endswith(".csv"):
                    preview = _parse_wide_csv(uploaded)
                else:
                    preview = _parse_wide_excel_first_sheet(uploaded)
                q_texts = [q.get("text", "")
                                 for q in preview.get("questions", [])]
                st.markdown(f"**문항 수: {len(q_texts)}개**")
                if q_texts:
                    # 모든 문항을 번호와 함께 표시
                    st.markdown("##### 전체 문항 목록:")
                    for idx, q_text in enumerate(q_texts, 1):
                        st.markdown(f"{idx}. {q_text}")
                else:
                    st.info("문항을 찾지 못했습니다. 1행에 문항이 있는지 확인하세요.")
            except Exception as e:
                st.warning(f"와이드 미리보기 실패: {str(e)}")
        else:
            if has_course:
                st.markdown("**Course** 미리보기")
                st.dataframe(dfs["course"].head(10))
            if has_questions:
                st.markdown("**Questions** 미리보기")
                st.dataframe(dfs["questions"].head(10))
            if has_responses:
                st.markdown("**Responses** 미리보기")
                st.dataframe(dfs["responses"].head(10))
            if not (has_course or has_questions or has_responses):
                st.warning("유효한 시트를 찾지 못했습니다. 헤더를 확인해주세요.")

    # 필수: 코스 메타데이터 입력 (항상 표시)
    st.markdown("### 📋 필수: 코스 메타데이터 입력")
    st.caption("⚠️ 파일 업로드 시 과정 정보를 반드시 입력해야 합니다.")

    # on_change 콜백: 사용자가 편집했음을 마킹
    def mark_course_id_edited():
        st.session_state.course_id_user_edited = True

    # v2 스키마 입력 폼 (모두 세션 상태 기반)
    col1, col2 = st.columns(2)
    with col1:
        st.text_input(
            "과정 ID (필수)*",
            value=st.session_state.upload_course_id,
            key="meta_course_id",
            on_change=mark_course_id_edited,
            placeholder="예: C-2025-001",
            help="고유한 과정 식별자 (자동 생성되지만 수정 가능)"
        )
        st.text_input(
            "프로그램명 (필수)*",
            value="",
            key="meta_program_name",
            placeholder="예: Next Chip Talk",
            help="필수 입력 항목입니다."
        )
        st.text_input(
            "회차 (필수)*",
            value="1",
            key="meta_session_no",
            help="필수 입력 항목입니다."
        )
        st.text_input(
            "주제 (필수)*",
            value="",
            key="meta_theme",
            placeholder="예: AI 반도체 기술",
            help="필수 입력 항목입니다."
        )

    with col2:
        st.selectbox(
            "이벤트 유형 (필수)*",
            ["NCT", "Forum", "Workshop", "Webinar", "Internal Talk"],
            index=0,
            key="meta_event_type"
        )
        st.date_input("행사 날짜", key="meta_event_date")
        st.text_input("장소", value="온라인", key="meta_location")
        st.text_input("주최/주관", value="SK hynix", key="meta_host_org")

    st.text_input(
        "연사 (세미콜론 구분)",
        value="",
        key="meta_speakers",
        placeholder="예: 김박사;이교수"
    )
    st.selectbox(
        "상태",
        ["planned", "active", "completed", "archived"],
        index=1,  # 기본: active
        key="meta_status"
    )

    # 버튼은 항상 활성화 (검증은 클릭 후)
    if st.button("업로드 실행", type="primary"):
        # ============================================
        # 1. 필수 필드 검증 (submit 후에만 수행)
        # ============================================

        # 디버그: 세션 상태 확인
        debug_state = {
            "meta_course_id": st.session_state.get("meta_course_id"),
            "meta_program_name": st.session_state.get("meta_program_name"),
            "meta_session_no": st.session_state.get("meta_session_no"),
            "meta_theme": st.session_state.get("meta_theme"),
            "meta_event_type": st.session_state.get("meta_event_type"),
        }

        # 개발 중에만 표시 (프로덕션에서는 주석 처리)
        with st.expander("🔍 디버그: 세션 상태 확인"):
            st.json(debug_state)

        # 안전하게 문자열로 변환
        required_fields = {
            "course_id": safe_str(st.session_state.get("meta_course_id")),
            "program_name": safe_str(st.session_state.get("meta_program_name")),
            "session_no": safe_str(st.session_state.get("meta_session_no")),
            "theme": safe_str(st.session_state.get("meta_theme")),
            "event_type": safe_str(st.session_state.get("meta_event_type")),
        }

        # 디버그: 변환 후 값 확인
        with st.expander("🔍 디버그: 변환 후 필드 값"):
            st.json(required_fields)

        missing = [k for k, v in required_fields.items() if not v]

        if missing:
            st.error(f"❌ 필수 항목을 입력해주세요: {', '.join(missing)}")
            st.warning("⚠️ 모든 필수 항목(*)을 입력한 후 다시 시도하세요.")

            # 디버그: 누락된 필드의 원본 값 표시
            with st.expander("🔍 디버그: 누락 필드 상세"):
                for field in missing:
                    raw_val = st.session_state.get(f"meta_{field}")
                    st.write(f"- {field}:")
                    st.write(f"  원본: {repr(raw_val)}")
                    st.write(f"  타입: {type(raw_val)}")
                    st.write(f"  변환 후: {repr(required_fields[field])}")

                st.stop()

        # ============================================
        # 2. 업로드 시작
        # ============================================
        try:
            log_box = st.expander("업로드 로그", expanded=False)
            # Helper: exponential backoff wrapper with API quota handling

            def _with_backoff(fn, *args, **kwargs):
                delays = [2, 4, 8, 16, 32]  # Increased delays for quota limits
                last_err = None
                for i, d in enumerate([0] + delays):
                    if d:
                        time.sleep(d)
                        with log_box:
                            st.write(f"⏳ API 쿼터 제한으로 {d}초 대기 중...")
                    try:
                        return fn(*args, **kwargs)
                    except Exception as e:
                        msg = str(e)
                        last_err = e
                        if ("429" in msg) or ("Quota exceeded" in msg) or (
                            "quota" in msg.lower()):
                            with log_box:
                                st.write(
                                    f"⚠️ API 쿼터 초과 감지 (시도 {i + 1}/{len(delays)})")
                            continue
                        raise
                raise last_err

            # 3) Course 저장 (v2 스키마 사용)
            course_saved_id = None

            # 세션 상태에서 안전하게 값 가져오기
            course_id_final = required_fields["course_id"]

            # event_date 처리 (safe_date 사용)
            event_date_val = st.session_state.get("meta_event_date")
            event_date_str = safe_date(event_date_val)

            # v2 스키마 객체 생성 (세션 상태 기반, 사용자 입력값 절대 덮어쓰지 않음)
            course_obj_v2 = {
                "course_id": course_id_final,  # 문자열로 보장
                # 이미 safe_str 적용됨
                "program_name": required_fields["program_name"],
                # 이미 safe_str 적용됨
                "session_no": required_fields["session_no"],
                # 이미 safe_str 적용됨
                "theme": required_fields["theme"],
                # 이미 safe_str 적용됨
                "event_type": required_fields["event_type"],
                "event_date": event_date_str,                     # safe_date 적용됨
                "location": safe_str(st.session_state.get("meta_location")),
                "host_org": safe_str(st.session_state.get("meta_host_org")),
                "speakers": safe_str(st.session_state.get("meta_speakers")),
                "survey_form_version": "v2.0",
                "response_source_file": uploaded.name if uploaded else "",
                "status": safe_str(st.session_state.get("meta_status", "active")),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            # 디버그: 최종 course 객체 확인
            with log_box:
                st.write("🔍 디버그: 최종 course 객체")
                st.json({k: v for k, v in course_obj_v2.items() if k in [
                        "course_id", "program_name", "theme", "event_type", "event_date"]})

            _with_backoff(upsert_course_v2, spreadsheet, course_obj_v2)
            course_saved_id = course_obj_v2["course_id"]

            # 세션 상태 동기화 (다음 렌더링 시에도 유지)
            st.session_state.upload_course_id = course_id_final

            with log_box:
                st.write(
    f"✅ Course 저장: course_id={course_saved_id}, program_name='{
        course_obj_v2.get(
            'program_name', '')}'")
                st.write(
    f"   이벤트: {
        course_obj_v2['event_type']} / 회차: {
            course_obj_v2['session_no']} / 날짜: {
                course_obj_v2['event_date']}")

            # 4) 헤더 기반 Survey_Items 자동 등록 및 매핑
            with log_box:
                st.write("📝 파일 헤더 추출 중...")

            # 파일 다시 읽기 (헤더 추출용)
            try:
                # 파일 포인터를 처음으로 되돌리기
                uploaded.seek(0)

                # 🔧 파일 시그니처 확인 (실제 파일 형식 감지)
                file_content = uploaded.read()
                uploaded.seek(0)
                
                is_zip_based = file_content[:2] == b'PK'  # ZIP/XLSX 시그니처
                
                if is_zip_based:
                    # 실제로 XLSX 파일
                    with log_box:
                        st.info("💡 파일 시그니처 확인: XLSX 형식 (ZIP 기반)")
                    df_headers = pd.read_excel(io.BytesIO(file_content), nrows=0, engine='openpyxl')
                else:
                    # 실제로 CSV 파일 - 다중 인코딩 시도
                    with log_box:
                        st.info("💡 파일 시그니처 확인: CSV 형식")
                    
                    df_headers = None
                    for encoding in ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8', 'latin-1']:
                        try:
                            df_headers = pd.read_csv(io.BytesIO(file_content), nrows=0, encoding=encoding)
                            with log_box:
                                st.success(f"✅ 헤더 읽기 성공: {encoding}")
                            break
                        except Exception:
                            continue
                    
                    if df_headers is None:
                        raise ValueError("CSV 헤더를 읽을 수 없습니다. 파일 인코딩을 확인하세요.")

                headers = list(df_headers.columns)

                with log_box:
                    st.write(f"📋 총 {len(headers)}개 컬럼 발견")

                # Survey_Items 자동 등록
                with log_box:
                    st.write("🔍 설문 문항 자동 등록 중...")

                registered_items = _with_backoff(
                    ensure_survey_items_from_headers,
                    spreadsheet,
                    headers
                )

                with log_box:
                    st.write(f"✅ {len(registered_items)}개 문항 등록 완료")
                    for item in registered_items[:5]:  # 처음 5개만 표시
                        st.write(
                            f"   - {item.get('item_text', '')[:50]} ({item.get('metric_type', '')})")
                    if len(registered_items) > 5:
                        st.write(f"   ... 외 {len(registered_items) - 5}개")

                # Course ↔ Items 매핑
                if registered_items:
                    with log_box:
                        st.write("🧹 기존 과정-문항 매핑 정리 중...")

                    removed_count = _with_backoff(
                        delete_course_item_mappings,
                        spreadsheet,
                        course_saved_id,
                    )

                    with log_box:
                        st.write(f"   - 기존 매핑 {removed_count}개 삭제")

                    with log_box:
                        st.write("🔗 새 과정-문항 매핑 생성 중...")

                    _with_backoff(
                        ensure_course_item_mapping,
                        spreadsheet,
                        course_saved_id,
                        registered_items
                    )

                    with log_box:
                        st.write(f"✅ {len(registered_items)}개 문항 매핑 완료")

            except Exception as e:
                with log_box:
                    st.warning(f"⚠️ 헤더 기반 자동 등록 실패: {str(e)}")
                    st.write("💡 수동으로 Survey_Items를 등록해야 할 수 있습니다.")

            # 2) Questions 저장 (표준 또는 와이드 포맷)
            imported_questions = 0
            wide_result = {"questions": [], "responses": []}
            
            # 💡 와이드 포맷 파싱 시작 - 파일 시그니처로 실제 형식 감지
            if use_wide_format:
                # 🔧 파일 시그니처로 실제 형식 확인
                uploaded.seek(0)
                file_magic = uploaded.read(4)
                uploaded.seek(0)
                
                is_zip_based = file_magic[:2] == b'PK'
                
                if is_zip_based:
                    # 실제로 XLSX 파일 (확장자와 무관하게)
                    with log_box:
                        st.write("📊 엑셀 와이드 포맷 파싱 중...")
                        st.info(f"💡 파일 시그니처: XLSX (실제 확장자: {uploaded.name.split('.')[-1]})")
                        if not uploaded.name.lower().endswith(".xlsx"):
                            st.warning("⚠️ 파일 확장자는 .csv이지만 실제로는 XLSX 파일입니다!")
                    
                    wide_result = _parse_wide_excel_first_sheet(uploaded)
                    
                    # 파싱 실패 시 (questions가 없으면)
                    if not wide_result.get("questions"):
                        st.error("❌ XLSX 파일 파싱 실패!")
                        st.error("🚨 **필수 조치**: Excel에서 파일을 열고 CSV UTF-8로 저장 후 재업로드하세요!")
                        return
                    
                    # 🚨 메타데이터 열 건너뛰기 알림
                    if wide_result.get("skipped_columns"):
                        with log_box:
                            st.info(f"📋 메타데이터/PII 열 건너뛰기: {len(wide_result['skipped_columns'])}개")
                            with st.expander("🔍 건너뛴 열 목록 보기"):
                                for col in wide_result["skipped_columns"]:
                                    st.write(f"   - {col}")
                                st.caption("💡 이 열들은 응답자 개인정보로 간주되어 문항으로 등록되지 않았습니다.")
                else:
                    # 실제로 CSV 파일 (가장 안정적)
                    with log_box:
                        st.write("📊 CSV 와이드 포맷 파싱 중...")
                        st.info("💡 파일 시그니처: CSV")
                    
                    wide_result = _parse_wide_csv(uploaded)
                    
                    # 🚨 메타데이터 열 건너뛰기 알림
                    if wide_result.get("skipped_columns"):
                        with log_box:
                            st.info(f"📋 메타데이터/PII 열 건너뛰기: {len(wide_result['skipped_columns'])}개")
                            with st.expander("🔍 건너뛴 열 목록 보기"):
                                for col in wide_result["skipped_columns"]:
                                    st.write(f"   - {col}")
                                st.caption("💡 이 열들은 응답자 개인정보로 간주되어 문항으로 등록되지 않았습니다.")
            
            # 💡 와이드 포맷 Questions 등록 (Excel/CSV 공통 처리)
            # 🚨 핵심 수정: 파일 헤더 텍스트를 registered_items의 item_text와 매핑하여 실제 item_id 사용
            question_text_to_item_id = {}  # 매핑 딕셔너리
            
            if use_wide_format and wide_result["questions"]:
                # 1. 질문 텍스트와 item_id 매핑 생성
                if registered_items:
                    with log_box:
                        st.write("🔍 파일 헤더를 Survey_Items의 item_id와 매핑 중...")
                    
                    for q in wide_result["questions"]:
                        q_text = q.get("text", "").strip()
                        # registered_items에서 매칭되는 item_text 찾기
                        matched_item = None
                        for item in registered_items:
                            item_text = item.get("item_text", "").strip()
                            # 텍스트 유사도 비교 (정규화된 비교)
                            if q_text and item_text:
                                # 간단한 매칭: 앞 50자 비교 또는 전체 텍스트 포함 여부
                                if q_text[:50] in item_text or item_text[:50] in q_text:
                                    matched_item = item
                                    break
                        
                        if matched_item:
                            # 매핑 성공: 실제 item_id 사용
                            item_id = matched_item.get("item_id")
                            question_text_to_item_id[q["questionId"]] = item_id
                            q["questionId"] = item_id  # 🚨 임시 ID를 실제 item_id로 교체
                            
                            with log_box:
                                st.write(f"   ✓ '{q_text[:40]}...' → {item_id}")
                        else:
                            # 매칭 실패: 경고 표시
                            with log_box:
                                st.warning(f"   ⚠️ '{q_text[:40]}...' - Survey_Items에서 매칭 실패")
                
                # 2. Questions 시트 등록 (레거시 호환용 - v2 스키마에서는 optional)
                # 💡 v2 스키마에서는 Survey_Items만 사용하므로, Questions 시트 없어도 OK
                with log_box:
                    st.info("💡 v2 스키마: Survey_Items만 사용하므로 Questions 시트는 건너뜁니다.")
                
                # Questions 카운트는 Survey_Items 기준으로
                imported_questions = len(wide_result["questions"])
            elif has_questions:
                q_df = dfs["questions"].fillna("")
                for _, r in q_df.iterrows():
                    q = _normalize_question_row(r.to_dict())
                    if not q.get("courseId"):
                        q["courseId"] = course_saved_id
                    if not q.get("order"):
                        q["order"] = "0"
                    _with_backoff(upsert_question, spreadsheet, q)
                    imported_questions += 1
                    # Add delay every 10 questions to avoid quota limits
                    if imported_questions % 10 == 0:
                        time.sleep(1)
                    with log_box:
                        st.write(
    f"Questions 등록: questionId={
        q['questionId']}, order={
            q['order']}, text='{
                q.get(
                    'text', '')[
                        :60]}'")

            # 3) Responses 저장 (표준 또는 와이드 포맷) - v2 스키마 사용
            imported_responses = 0
            if use_wide_format and wide_result["responses"]:
                # 각 응답자 묶음별로 동일 respondent_id를 유지하기 위해 index 단위로 그룹핑
                from collections import defaultdict
                idx_to_resps = defaultdict(list)
                for r in wide_result["responses"]:
                    idx_to_resps[r["respondentIndex"]].append(r)
                
                # 🆕 원본 데이터에서 응답자 메타데이터(회사명 등) 추출을 위한 준비
                respondent_metadata = {}  # {respondent_index: {"company": "...", ...}}
                
                try:
                    # 원본 파일에서 메타데이터 열 추출
                    uploaded.seek(0)
                    file_magic_meta = uploaded.read(4)
                    uploaded.seek(0)
                    
                    is_zip_based_meta = file_magic_meta[:2] == b'PK'
                    
                    if is_zip_based_meta:
                        # 실제로 XLSX 파일
                        df_meta = pd.read_excel(uploaded, header=0, engine='openpyxl')
                    else:
                        # 실제로 CSV 파일 - 다중 인코딩 시도
                        df_meta = None
                        for encoding in ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8', 'latin-1']:
                            try:
                                uploaded.seek(0)
                                df_meta = pd.read_csv(uploaded, encoding=encoding)
                                break
                            except Exception:
                                continue
                        if df_meta is None:
                            raise ValueError("CSV 메타데이터를 읽을 수 없습니다.")
                    
                    # 회사명 열 찾기
                    company_col = None
                    for col in df_meta.columns:
                        if "회사" in str(col) or "소속" in str(col) or "company" in str(col).lower():
                            company_col = col
                            break
                    
                    # 각 응답자의 회사명 추출 및 정규화
                    if company_col:
                        for idx in range(len(df_meta)):
                            company_raw = df_meta.loc[idx, company_col] if company_col in df_meta.columns else ""
                            respondent_metadata[idx] = {
                                "company": normalize_company_name(str(company_raw)) if pd.notna(company_raw) else ""
                            }
                        with log_box:
                            st.write(f"✅ 회사명 정규화 완료: {len(respondent_metadata)}개 응답자")
                except Exception as e:
                    with log_box:
                        st.warning(f"⚠️ 응답자 메타데이터 추출 실패: {str(e)}")
                
                op_count = 0
                for respondent_index, resp_list in sorted(idx_to_resps.items(), key=lambda x: x[0]):
                    # 🚨 수정: respondentIndex 기반으로 일관된 respondent_id를 생성
                    # 파일 업로드에서 고유한 사용자 해시를 생성하여 respondent_id로 사용
                    respondent_hash_key = f"upload_{course_saved_id}_{respondent_index}"
                    respondent_id = f"U-{hashlib.md5(respondent_hash_key.encode()).hexdigest()[:10]}"
                    
                    # 🆕 응답자 정보 저장 (v2 스키마 - Respondents 시트)
                    respondent_info = respondent_metadata.get(respondent_index, {})
                    try:
                        respondent_data = {
                            "respondent_id": respondent_id,
                            "course_id": course_saved_id,
                            "pii_consent": "",
                            "company": respondent_info.get("company", ""),
                            "department": "",
                            "job_role": "",
                            "tenure_years": "",
                            "name": "",
                            "phone": "",
                            "email": "",
                            "hashed_contact": "",
                            "extra_meta": "",
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        }
                        _with_backoff(save_respondent, spreadsheet, respondent_data)
                        
                        if respondent_info.get("company"):
                            with log_box:
                                st.write(f"   Respondent {respondent_id}: {respondent_info['company']}")
                    except Exception as e:
                        with log_box:
                            st.warning(f"⚠️ 응답자 정보 저장 실패 (ID: {respondent_id}): {str(e)}")
                    
                    for r in resp_list:
                        # 🚨 v2 스키마로 응답 데이터 구성
                        # 🔑 핵심: 임시 questionId를 매핑된 실제 item_id로 변환
                        original_qid = r["questionId"]
                        actual_item_id = question_text_to_item_id.get(original_qid, original_qid)
                        
                        answer_str = str(r["answer"]) if r["answer"] else ""
                        
                        # 숫자 변환 시도
                        response_value_num = None
                        try:
                            if answer_str.strip():
                                response_value_num = float(answer_str)
                        except (ValueError, TypeError):
                            pass
                        
                        response_data = {
                            "response_id": generate_response_id(),
                            "course_id": course_saved_id,
                            "respondent_id": respondent_id,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "item_id": actual_item_id,  # 🚨 매핑된 실제 item_id 사용
                            "response_value": answer_str,
                            "response_value_num": response_value_num,
                            "choice_value": "",
                            "comment_text": answer_str if r.get("type") == "subjective" else "",
                            "source_row_index": str(respondent_index + 2),  # 헤더 제외한 행 번호
                            "ingest_batch_id": generate_batch_id(),
                        }
                        
                        # 🚨 save_response_v2 호출로 변경
                        _with_backoff(save_response_v2, spreadsheet, response_data)
                        imported_responses += 1
                        op_count += 1
                        
                        # More frequent delays to avoid quota limits
                        if op_count % 20 == 0:
                            time.sleep(1.5)
                            with log_box:
                                st.write(f"⏸️ API 쿼터 보호: 20개 작업마다 1.5초 대기")
                        
                        with log_box:
                            st.write(
                                f"Responses 등록 (v2): item_id={actual_item_id}, "
                                f"answer='{answer_str[:60]}', respondent_id={respondent_id}"
                            )
            elif has_responses:
                r_df = dfs["responses"].fillna("")
                op_count = 0
                for row_idx, r in r_df.iterrows():
                    resp = _normalize_response_row(r.to_dict())
                    # courseId 보정
                    if not resp.get("courseId"):
                        resp["courseId"] = course_saved_id
                    
                    # 🚨 v2 스키마로 응답 데이터 구성
                    answer_str = str(resp.get("answer", ""))
                    
                    # 숫자 변환 시도
                    response_value_num = None
                    try:
                        if answer_str.strip():
                            response_value_num = float(answer_str)
                    except (ValueError, TypeError):
                        pass
                    
                    # respondentHash를 respondent_id로 변환
                    respondent_id = f"U-{resp.get('respondentHash', 'unknown')[:10]}"
                    
                    response_data = {
                        "response_id": generate_response_id(),
                        "course_id": resp["courseId"],
                        "respondent_id": respondent_id,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "item_id": resp["questionId"],  # questionId를 item_id로 사용
                        "response_value": answer_str,
                        "response_value_num": response_value_num,
                        "choice_value": "",
                        "comment_text": answer_str,  # 모든 응답을 comment로 저장
                        "source_row_index": str(row_idx + 2),  # 헤더 제외한 행 번호
                        "ingest_batch_id": generate_batch_id(),
                    }
                    
                    # 🚨 save_response_v2 호출로 변경
                    _with_backoff(save_response_v2, spreadsheet, response_data)
                    imported_responses += 1
                    op_count += 1
                    
                    # More frequent delays to avoid quota limits
                    if op_count % 20 == 0:
                        time.sleep(1.5)
                        with log_box:
                            st.write(f"⏸️ API 쿼터 보호: 20개 작업마다 1.5초 대기")
                    
                    with log_box:
                        st.write(
                            f"Responses 등록 (v2): item_id={resp['questionId']}, "
                            f"answer='{answer_str[:60]}', respondent_id={respondent_id}"
                        )

            # 4) 통계 갱신 (v2 스키마에서는 optional - Questions 시트 필요 없음)
            if course_saved_id:
                try:
                    update_response_stats(spreadsheet, course_saved_id)
                    with log_box:
                        st.write("✅ ResponseStats 업데이트 완료")
                except Exception as stats_err:
                    with log_box:
                        st.info(f"💡 ResponseStats 업데이트 건너뜀 (v2 스키마에서는 불필요): {str(stats_err)[:100]}")

            st.success(
                f"✅ 업로드 완료!\n\n"
                f"- 코스ID: **{course_saved_id}**\n"
                f"- 프로그램: **{course_obj_v2.get('program_name')}**\n"
                f"- 질문: {imported_questions}개\n"
                f"- 응답: {imported_responses}개"
            )

            # 캐시 클리어 (리스트 즉시 갱신)
            st.cache_data.clear()

            # 세션 상태 클리어 (다음 업로드를 위해)
            st.session_state.upload_course_id = ""
            st.session_state.course_id_user_edited = False

            st.balloons()
            st.info("💡 '과정 리스트' 탭으로 이동하여 새로 추가된 과정을 확인하세요!")

        except Exception as e:
            import traceback
            st.error(f"❌ 업로드 중 오류: {str(e)}")
            with st.expander("🔍 상세 에러 정보 (디버깅용)"):
                st.code(traceback.format_exc())


def page_course_list(spreadsheet, is_admin: bool):
    st.markdown(
        """
        <div class="sk-page-header">
          <div class="title">mySUNI 교육 과정 List</div>
          <div class="subtitle">카테고리별 교육 과정을 찾아보세요</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # v2 스키마 시도
    try:
        rows = list_courses_v2(spreadsheet, status=None)  # 모든 상태
        # v2 필드 사용
        use_v2 = True
    except Exception:
        rows = get_all_courses_cached(spreadsheet)
        use_v2 = False

    # 필터링: program_name/title이 있는 것만
    if use_v2:
        valid_rows = [
    r for r in rows if str(
        r.get(
            "program_name",
             "")).strip()]
    else:
        valid_rows = [r for r in rows if str(r.get("title", "")).strip()]

    if not valid_rows:
        st.info("등록된 과정이 없습니다.")
        if is_admin:
            with st.expander("새 과정 만들기"):
                _course_create_form_v2(
                    spreadsheet) if use_v2 else _course_create_form(spreadsheet)
        return

    # Group by category/event_type
    categories = {}
    for r in valid_rows:
        if use_v2:
            cat = (r.get("event_type") or "기본").strip()
        else:
            cat = (r.get("category") or "기본").strip()
        categories.setdefault(cat, []).append(r)

    for cat, items in categories.items():
        st.markdown(f"### {cat}")
        cols = st.columns(3)
        for i, row in enumerate(items):
            with cols[i % 3]:
                # v2 vs 레거시 필드
                if use_v2:
                    title = row.get('program_name', '(제목없음)')
                    desc = f"{
    row.get(
        'theme',
        '')} | {
            row.get(
                'session_no',
                '')}회차 | {
                    row.get(
                        'event_date',
                         '')}"
                    course_id = row.get('course_id')
                else:
                    title = row.get('title', '(제목없음)')
                    desc = row.get('description', '')
                    course_id = row.get('courseId')

                st.markdown(
                    f"""
                    <div class='sk-card'>
                      <h4>{title}</h4>
                      <div class='sk-desc'>{desc}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                if is_admin:
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("설문 편집", key=f"edit_{course_id}"):
                            st.session_state["editing_course_id"] = str(
                                course_id)
                            st.session_state["viewing_dashboard"] = None
                    with c2:
                        if st.button("결과 보기", key=f"result_{course_id}"):
                            st.session_state["viewing_dashboard"] = str(
                                course_id)
                            st.session_state["editing_course_id"] = None
                else:
                    st.button("설문 참여", key=f"join_{course_id}")

    # Editor panel if a course is selected
    if is_admin and st.session_state.get("editing_course_id"):
        st.divider()
        _course_editor(spreadsheet, st.session_state["editing_course_id"])

    # Dashboard if viewing results
    if is_admin and st.session_state.get("viewing_dashboard"):
        st.divider()
        page_dashboard(spreadsheet, st.session_state["viewing_dashboard"])


def _course_create_form(spreadsheet):
    with st.form("create_course"):
        course: Dict[str, str] = {}
        course["courseId"] = st.text_input(
            "과정ID", value=str(int(datetime.utcnow().timestamp())))
        course["title"] = st.text_input("제목")
        course["description"] = st.text_area("설명")
        course["category"] = st.text_input("카테고리", value="기본")
        course["createdAt"] = datetime.utcnow().isoformat()
        course["status"] = st.selectbox("상태", ["active", "inactive"], index=0)
        course["ownerId"] = st.text_input("관리자ID", value="admin")
        submitted = st.form_submit_button("저장")
        if submitted:
            upsert_course(spreadsheet, course)
            st.success("과정이 저장되었습니다. 새로고침하여 확인하세요.")


def _course_create_form_v2(spreadsheet):
    """v2 스키마 과정 생성 폼"""
    with st.form("create_course_v2"):
        st.markdown("### 새 과정 만들기 (v2)")

        col1, col2 = st.columns(2)

        with col1:
            course_id = st.text_input(
    "과정 ID*", value=generate_course_id(), help="고유 식별자")
            program_name = st.text_input(
    "프로그램명*", placeholder="Next Chip Talk")
            session_no = st.text_input("회차*", value="1")
            theme = st.text_input("주제*", placeholder="AI 반도체 설계")

        with col2:
            event_type = st.selectbox(
                "이벤트 유형*", ["NCT", "Forum", "Workshop", "Webinar", "Internal Talk"], index=0)
            event_date = st.date_input("행사 날짜*")
            location = st.text_input("장소", value="온라인")
            host_org = st.text_input("주최/주관", value="SK hynix")

        speakers = st.text_input("연사 (세미콜론 구분)", placeholder="김박사;이교수")
        status = st.selectbox(
            "상태*", ["planned", "active", "completed", "archived"], index=0)

        submitted = st.form_submit_button("과정 저장")

        if submitted:
            if not all([course_id, program_name,
                       session_no, theme, event_type]):
                st.error("❌ 필수 항목(*)을 모두 입력해주세요.")
                return

            course = {
                "course_id": course_id,
                "program_name": program_name,
                "session_no": session_no,
                "theme": theme,
                "event_type": event_type,
                "event_date": event_date.isoformat() if event_date else "",
                "location": location,
                "host_org": host_org,
                "speakers": speakers,
                "survey_form_version": "v2.0",
                "response_source_file": "",
                "status": status,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            try:
                upsert_course_v2(spreadsheet, course)
                st.success("✅ 과정이 저장되었습니다!")
                st.cache_data.clear()  # 캐시 클리어
                st.balloons()
            except Exception as e:
                st.error(f"저장 실패: {str(e)}")


def _course_editor(spreadsheet, course_id: str):
    st.markdown(f"#### 설문 편집기 · Course ID: {course_id}")
    settings = get_survey_settings(spreadsheet, course_id)
    active_now = str(settings.get("isActive", "FALSE")).upper() == "TRUE"
    col1, col2 = st.columns([1, 3])
    with col1:
        new_state = st.toggle("설문 활성화", value=active_now)
        if new_state != active_now:
            set_survey_active(spreadsheet, course_id, new_state)
            st.toast("설문 활성화 상태가 업데이트되었습니다.")
    with col2:
        st.write("미리보기(간단)")
        _render_preview(spreadsheet, course_id)

    st.markdown("##### 문항 목록")

    # v2 스키마 시도 (Course_Item_Map + Survey_Items)
    try:
        questions = get_course_items(spreadsheet, course_id)
        is_v2 = True
    except Exception:
        # 레거시 스키마로 폴백
        questions = list_questions(spreadsheet, course_id)
        is_v2 = False

    if not questions:
        st.info("문항이 없습니다. 아래에서 추가하세요.")
    else:
        for q in questions:
            q_col1, q_col2 = st.columns([4, 1])
            with q_col1:
                # v2: metric_type, item_text 사용 / 레거시: type, text 사용
                q_type = q.get('metric_type') if is_v2 else q.get('type')
                q_order = q.get('order_in_course') if is_v2 else q.get('order')
                q_text = q.get('item_text') if is_v2 else q.get('text')
                st.markdown(f"- ({q_type}) [{q_order}] {q_text}")
            with q_col2:
                q_id = q.get('item_id') if is_v2 else q.get('questionId')
                if st.button("삭제", key=f"del_{q_id}"):
                    if delete_question(spreadsheet, str(q_id)):
                        st.experimental_rerun()

    st.markdown("##### 문항 추가")
    with st.form("add_question"):
        q = {}
        q["questionId"] = st.text_input("문항ID", value=str(
            int(datetime.utcnow().timestamp() * 1000)))
        q["courseId"] = course_id
        q["order"] = st.number_input(
    "표시 순서", min_value=1, value=(
        len(questions) + 1))
        q["text"] = st.text_input("문항 내용")
        q_type = st.selectbox(
    "문항 유형", [
        "objective", "subjective", "rating"], index=0)
        q["type"] = q_type
        if q_type == "objective":
            choices = st.text_area("선택지(쉼표로 구분)")
            q["choicesJson"] = "[" + ",".join([f'\"{c.strip()}\"' for c in choices.split(
                ',') if c.strip()]) + "]" if choices else "[]"
        else:
            q["choicesJson"] = "[]"
        if q_type == "rating":
            q["ratingMax"] = str(
    st.number_input(
        "최대 평점",
        min_value=3,
        max_value=10,
         value=5))
        else:
            q["ratingMax"] = ""
        q["isRequired"] = "TRUE" if st.checkbox("필수 문항") else "FALSE"
        q["maxChars"] = str(
    st.number_input(
        "최대 글자 수(주관식)",
        min_value=0,
         value=0))
        submitted = st.form_submit_button("문항 추가")
        if submitted:
            upsert_question(spreadsheet, q)
            st.success("문항이 추가되었습니다.")
            st.experimental_rerun()


def _render_preview(spreadsheet, course_id: str):
    # v2 스키마 시도
    try:
        questions = get_course_items(spreadsheet, course_id)
        is_v2 = True
    except Exception:
        questions = list_questions(spreadsheet, course_id)
        is_v2 = False

    if not questions:
        st.caption("미리보기할 문항이 없습니다.")
        return

    for q in questions:
        if is_v2:
            # v2 스키마
            order = q.get('order_in_course', '0')
            text = q.get('item_text', '')
            q_type = q.get('metric_type', 'text')
            q_id = q.get('item_id', '')

            st.write(f"{order}. {text}")

            if q_type == "single_choice" or q_type == "multi_choice":
                options_str = q.get("options", "[]")
                try:
                    options = json.loads(options_str) if options_str else []
                except:
                    options = []
                if options:
                    st.radio(" ", options=options, key=f"prev_{q_id}")
            elif q_type == "likert" or q_type == "nps":
                scale_max = int(q.get("scale_max") or 5)
                st.slider(
    " ",
    min_value=1,
    max_value=scale_max,
    value=(
        scale_max + 1) // 2,
         key=f"prev_{q_id}")
            else:
                st.text_input(" ", key=f"prev_{q_id}")
        else:
            # 레거시 스키마
            st.write(f"{q.get('order')}. {q.get('text')}")
        t = q.get("type")
        if t == "objective":
            choices_str = q.get("choicesJson", "[]")
            try:
                choices = json.loads(choices_str) if choices_str else []
            except:
                choices = []
            if choices:
                st.radio(
    " ", options=choices, key=f"prev_{
        q.get('questionId')}")
        elif t == "rating":
            st.slider(" ", min_value=1, max_value=int(q.get("ratingMax") or 5), value=int(
                (int(q.get("ratingMax") or 5) + 1) / 2), key=f"prev_{q.get('questionId')}")
        else:
            st.text_input(" ", key=f"prev_{q.get('questionId')}")


def generate_respondent_hash() -> str:
    """Generate a hash for respondent identification"""
    session_id = st.session_state.get("session_id", "default")
    timestamp = str(datetime.utcnow().timestamp())
    return hashlib.md5(f"{session_id}_{timestamp}".encode()).hexdigest()[:8]


def mask_ip_address(ip: str) -> str:
    """Mask IP address for privacy"""
    if not ip or ip == "unknown":
        return "***.***.***.***"
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.***.***"
    return "***.***.***.***"


def render_survey_form(spreadsheet, course_id: str):
    """Render the survey form for a specific course (v2 스키마 호환)"""

    # v2 스키마 시도
    try:
        course_v2 = get_course_by_id_v2(spreadsheet, course_id)
        if course_v2:
            use_v2 = True
        else:
            course = get_course_by_id(spreadsheet, course_id)
            use_v2 = False
    except Exception as e:
        st.error(f"❌ 과정 로딩 실패: {str(e)}")
        return

    if use_v2:
        if not course_v2:
            st.error(f"❌ 과정을 찾을 수 없습니다: {course_id}")
            return

        # v2 스키마: status 확인
        if str(course_v2.get("status", "")).strip().lower() != "active":
            st.warning("⚠️ 이 설문은 현재 비활성화 상태입니다.")
            st.caption("관리자에게 문의해주세요.")
            return

        course_title = f"{
    course_v2.get(
        'program_name',
        '')} - {
            course_v2.get(
                'theme',
                 '')}"
        course_desc = f"{
    course_v2.get(
        'event_type',
        '')} | {
            course_v2.get(
                'session_no',
                '')}회차 | {
                    course_v2.get(
                        'event_date',
                         '')}"
    else:
        if not course:
            st.error("❌ 과정을 찾을 수 없습니다.")
            return

        # 레거시 스키마: settings 확인
        settings = get_survey_settings(spreadsheet, course_id)
        if str(settings.get("isActive", "FALSE")).upper() != "TRUE":
            st.warning("⚠️ 이 설문은 현재 비활성화 상태입니다.")
            return

        course_title = course.get('title', '설문')
        course_desc = course.get('description', '')

    st.markdown(
        f"""
        <div class="sk-page-header" style="margin-top:8px;">
          <div class="title">{course_title} 설문에 참여해주세요!</div>
          <div class="subtitle">{course_desc}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # v2 스키마 시도
    try:
        questions = get_course_items(spreadsheet, course_id)
        is_v2 = True
    except Exception:
        questions = list_questions(spreadsheet, course_id)
        is_v2 = False

    if not questions:
        st.info("설문 문항이 없습니다.")
        return

    # Initialize session
    if "session_id" not in st.session_state:
        st.session_state["session_id"] = f"session_{
    int(
        datetime.utcnow().timestamp())}"

    # Render questions
    responses = {}
    for i, q in enumerate(questions):
        # v2 vs 레거시 필드 추출
        if is_v2:
            q_text = q.get('item_text', '')
            q_id = str(q.get('item_id'))
            is_required = str(q.get('is_required', 'FALSE')).upper() == 'TRUE'
            q_type = q.get('metric_type', 'text')
        else:
            q_text = q.get('text', '')
            q_id = str(q.get('questionId'))
            is_required = str(q.get('isRequired', 'FALSE')).upper() == 'TRUE'
            q_type = q.get('type', 'subjective')

        st.markdown(f"### {i + 1}. {q_text}")
        if is_required:
            st.markdown(
    "<span style='color:#D90B31;font-family: TheJamsil-4;'>*필수 문항*</span>",
     unsafe_allow_html=True)

        # v2 타입 매핑 (likert → rating, single_choice → objective)
        if is_v2:
            if q_type in ['likert', 'nps']:
                q_type = 'rating'
            elif q_type in ['single_choice', 'multi_choice']:
                q_type = 'objective'
            else:
                q_type = 'subjective'

        if q_type == 'objective':
            if is_v2:
                choices_str = q.get('options', '[]')
            else:
                choices_str = q.get('choicesJson', '[]')
            try:
                choices = json.loads(choices_str) if choices_str else []
            except:
                choices = []
            if choices:
                response = st.radio(
                    "선택하세요:",
                    options=choices,
                    key=f"q_{q_id}",
                    index=None
                )
                responses[q_id] = response or ""
            else:
                st.warning("선택지가 설정되지 않았습니다.")
                responses[q_id] = ""

        elif q_type == 'rating':
            if is_v2:
                max_rating = int(q.get('scale_max', 5))
            else:
                max_rating = int(q.get('ratingMax', 5))
            response = st.slider(
                f"평점을 선택하세요 (1-{max_rating}점):",
                min_value=1,
                max_value=max_rating,
                value=(max_rating + 1) // 2,
                key=f"q_{q_id}"
            )
            responses[q_id] = str(response)

        else:  # subjective
            max_chars = int(q.get('maxChars', 0)) if not is_v2 else 0
            char_limit = f" (최대 {max_chars}자)" if max_chars > 0 else ""
            response = st.text_area(
                f"답변을 입력하세요{char_limit}:",
                key=f"q_{q_id}",
                max_chars=max_chars if max_chars > 0 else None
            )
            responses[q_id] = response or ""

        st.divider()

    # Submit button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("설문 제출", type="primary", use_container_width=True):
            # Validate required fields
            missing_required = []
            for q in questions:
                if is_v2:
                    is_required = str(
    q.get(
        'is_required',
         'FALSE')).upper() == 'TRUE'
                    q_id = str(q.get('item_id'))
                    q_text = q.get(
    'item_text', f"문항 {
        q.get(
            'order_in_course', '')}")
                else:
                    is_required = str(
    q.get(
        'isRequired',
         'FALSE')).upper() == 'TRUE'
                    q_id = str(q.get('questionId'))
                    q_text = q.get('text', f"문항 {q.get('order', '')}")

                if is_required:
                    if not responses.get(
                        q_id) or responses[q_id].strip() == "":
                        missing_required.append(q_text)

            if missing_required:
                st.error(f"❌ 다음 필수 문항을 답해주세요: {', '.join(missing_required)}")
            else:
                # Save responses
                try:
                    if is_v2:
                        # v2 스키마: Respondents + Responses 저장
                        import uuid
                        respondent_id = f"U-{uuid.uuid4().hex[:10]}"

                        # Respondent 저장 (PII 없이)
                        respondent_data = {
                            "respondent_id": respondent_id,
                            "course_id": course_id,
                            "pii_consent": False,
                            "company": None,
                            "job_role": None,
                            "tenure_years": None,
                            "name": None,
                            "phone": None,
                            "email": None,
                            "hashed_contact": generate_respondent_hash(),
                            "extra_meta": None,
                        }

                        with st.spinner("설문을 제출하는 중..."):
                            save_respondent(spreadsheet, respondent_data)

                            # Responses 저장
                            batch_id = f"B-{
    datetime.now(
        timezone.utc).isoformat()}"
                            for idx, (item_id, answer) in enumerate(
                                responses.items()):
                                response_data = {
                                    "response_id": f"R-{uuid.uuid4().hex[:12]}",
                                    "course_id": course_id,
                                    "respondent_id": respondent_id,
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "item_id": item_id,
                                    "response_value": answer,
                                    "choice_value": None,
                                    "comment_text": None,
                                    "response_value_num": None,
                                    "source_row_index": None,
                                    "ingest_batch_id": batch_id,
                                }
                                save_response_v2(spreadsheet, response_data)

                                # Add small delay to avoid quota limits
                                if idx > 0 and idx % 5 == 0:
                                    time.sleep(0.5)
                    else:
                        # 레거시 스키마
                        respondent_hash = generate_respondent_hash()
                        session_id = st.session_state["session_id"]
                        ip_masked = mask_ip_address("unknown")

                        with st.spinner("설문을 제출하는 중..."):
                            for idx, (q_id, answer) in enumerate(
                                responses.items()):
                                save_response(
                                    spreadsheet, course_id, q_id, answer,
                                    respondent_hash, session_id, ip_masked
                                )
                                if idx > 0 and idx % 5 == 0:
                                    time.sleep(0.5)

                        # Update stats
                        update_response_stats(spreadsheet, course_id)

                    st.success("✅ 설문이 성공적으로 제출되었습니다! 소중한 의견 감사합니다.")
                    st.balloons()

                    # Clear form
                    for q_id in responses.keys():
                        if f"q_{q_id}" in st.session_state:
                            del st.session_state[f"q_{q_id}"]

                    # Clear cache
                    st.cache_data.clear()

                except Exception as e:
                    st.error(f"❌ 설문 제출 중 오류가 발생했습니다: {str(e)}")
                    st.exception(e)


def page_survey_participation(spreadsheet):
    """Page for survey participation (v2 스키마 호환)"""
    st.subheader("설문 참여")

    # ⛳️ 로컬에서만 쓰는 이름으로 초기화 (courses 금지)
    course_rows = []  # ← 최상단에서 초기화(모든 경로에서 존재)
    use_v2 = False
    
    # v2 스키마 시도
    try:
        courses_v2 = list_courses_v2(spreadsheet)
        use_v2 = bool(courses_v2)
    except Exception:
        use_v2 = False
        courses_v2 = []

    active_courses = []

    if use_v2:
        # v2 스키마: status가 'active'인 과정
        for course in courses_v2:
            if str(course.get('status', '')).strip().lower() == 'active':
                active_courses.append(course)
    else:
        # 레거시 스키마 - course_rows로 받기 (courses 금지!)
        course_rows = get_all_courses_cached(spreadsheet)
        for course in course_rows:
            settings = get_survey_settings(
                spreadsheet, str(course.get('courseId')))
            if str(settings.get('isActive', 'FALSE')).upper() == 'TRUE':
                active_courses.append(course)

    if not active_courses:
        st.info("📝 현재 참여 가능한 설문이 없습니다.")
        st.caption("관리자에게 문의하거나 나중에 다시 확인해주세요.")
        return

    # Course selection (don't auto-select; wait for explicit user choice)
    if use_v2:
        course_options = {
            f"{c.get('program_name', '')} - {c.get('theme', '')} (ID: {c.get('course_id')})":
            c.get('course_id')
            for c in active_courses
        }
    else:
        course_options = {
            f"{c.get('title', '')} (ID: {c.get('courseId')})":
            c.get('courseId')
            for c in active_courses
        }

    select_placeholder = "-- 설문을 선택하세요 --"
    select_options = [select_placeholder] + list(course_options.keys())
    selected_course_name = st.selectbox(
        "참여할 설문을 선택하세요:", select_options, index=0)

    if selected_course_name and selected_course_name != select_placeholder:
        selected_course_id = course_options[selected_course_name]
        st.divider()
        render_survey_form(spreadsheet, selected_course_id)


def configure_gemini():
    """Configure Gemini AI client"""
    try:
        from google import genai
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key and hasattr(
    st, "secrets") and "GEMINI_API_KEY" in st.secrets:
            api_key = st.secrets["GEMINI_API_KEY"]
        if not api_key:
            return None
        os.environ["GOOGLE_API_KEY"] = api_key
        return genai.Client()
    except Exception as e:
        st.warning(f"Gemini AI 설정 실패: {str(e)}")
        return None


def analyze_rating_data(
    spreadsheet,
    course_id: str,
    question: Dict,
    all_responses: List[Dict] = None) -> Dict:
    """Analyze rating-type question responses (v2 호환 로직)"""
    # ⚠️ 레거시 get_responses_by_question 호출은 생략 (all_responses 사용 권장)

    # v2 스키마에서 item_id를 사용하고, 없으면 레거시 questionId를 사용
    q_id = str(question.get('item_id') or question.get('questionId'))
    
    if all_responses is None:
        return {"no_data": True, "error": "All responses not provided."}

    # 캐시된 응답에서 해당 문항만 필터링 (v2는 item_id 사용)
    responses = [r for r in all_responses if str(r.get("item_id") or r.get("questionId")) == q_id]

    if not responses:
        return {"no_data": True}

    # Count ratings (safely convert to int)
    rating_counts = Counter()
    valid_ratings = []
    for r in responses:
        try:
            # v2 스키마는 response_value_num 사용 (숫자 응답)
            answer_v2_key = r.get("response_value_num")
            
            # v2 키를 우선 사용하고, 없으면 레거시 answer 키를 폴백
            answer = answer_v2_key if answer_v2_key is not None else r.get("answer", "")
            
            if answer is not None and str(answer).strip():
                rating = int(float(str(answer).strip())) # 안전하게 float 변환 후 int
                rating_counts[rating] += 1
                valid_ratings.append(rating)
        except (ValueError, TypeError):
            # 숫자로 변환할 수 없는 응답은 무시
            pass

    average = sum(valid_ratings) / len(valid_ratings) if valid_ratings else 0

    return {
        "no_data": False,
        "counts": dict(rating_counts),
        "total": len(valid_ratings), # 응답 수: 유효한 rating만 카운트
        "average": average
    }


def analyze_objective_data(
    spreadsheet,
    course_id: str,
    question: Dict,
    all_responses: List[Dict] = None) -> Dict:
    """Analyze objective-type question responses (v2 호환 로직)"""
    # ⚠️ 레거시 get_responses_by_question 호출은 생략

    # v2 스키마에서 item_id를 사용하고, 없으면 레거시 questionId를 사용
    q_id = str(question.get('item_id') or question.get('questionId'))

    if all_responses is None:
        return {"no_data": True, "error": "All responses not provided."}

    # 캐시된 응답에서 해당 문항만 필터링
    responses = [r for r in all_responses if str(r.get("item_id") or r.get("questionId")) == q_id]

    if not responses:
        return {"no_data": True}

    # Count choices (convert to string first)
    choice_counts = Counter()
    for r in responses:
        # v2 스키마는 response_value 또는 choice_value 사용 (선택지 텍스트)
        answer_v2_key = r.get("response_value") or r.get("choice_value")
        
        # v2 키를 우선 사용하고, 없으면 레거시 answer 키를 폴백
        answer = answer_v2_key if answer_v2_key is not None else r.get("answer", "")
        
        if answer is not None:
            answer_str = str(answer).strip()
            if answer_str:
                # multi_choice인 경우 쉼표로 분리하여 각 선택지를 카운트할 수 있지만, 
                # 여기서는 단일 문자열로 카운트하는 레거시 방식을 유지합니다.
                choice_counts[answer_str] += 1

    return {
        "no_data": False,
        "counts": dict(choice_counts),
        "total": len(responses)
    }


def analyze_subjective_data(
    spreadsheet,
    course_id: str,
    question: Dict,
    all_responses: List[Dict] = None) -> Dict:
    """Analyze subjective-type question responses (v2 호환 로직)"""
    # ⚠️ 레거시 get_responses_by_question 호출은 생략
    
    # v2 스키마에서 item_id를 사용하고, 없으면 레거시 questionId를 사용
    q_id = str(question.get('item_id') or question.get('questionId'))

    if all_responses is None:
        return {"no_data": True, "error": "All responses not provided."}

    # 캐시된 응답에서 해당 문항만 필터링
    responses = [r for r in all_responses if str(r.get("item_id") or r.get("questionId")) == q_id]

    if not responses:
        return {"no_data": True}

    # Collect text responses (convert to string first)
    texts = []
    for r in responses:
        # v2 스키마는 response_value 또는 comment_text 사용 (주관식 텍스트)
        answer_v2_key = r.get("response_value") or r.get("comment_text")
        
        # v2 키를 우선 사용하고, 없으면 레거시 answer 키를 폴백
        answer = answer_v2_key if answer_v2_key is not None else r.get("answer", "")
        
        if answer is not None:
            answer_str = str(answer).strip()
            if answer_str:
                texts.append(answer_str)

    return {
        "no_data": False,
        "responses": texts,
        "total": len(texts)
    }


def generate_wordcloud(texts: List[str]) -> plt.Figure:
    """Generate wordcloud from text list"""
    if not texts:
        return None

    combined_text = " ".join(texts)

    # Create wordcloud
    wc = WordCloud(
        width=800,
        height=400,
        background_color='white',
        font_path=None,  # Use default font for Korean
        colormap='RdYlBu_r',
        relative_scaling=0.5,
        min_font_size=10
    ).generate(combined_text)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wc, interpolation='bilinear')
    ax.axis('off')
    return fig


def generate_ai_insights(
    spreadsheet,
    course_id: str,
    questions: List[Dict],
    all_analysis: Dict) -> str:
    """Generate AI insights using Gemini (v2 호환 로직)"""
    client = configure_gemini()
    if not client:
        return "Gemini AI를 사용할 수 없습니다. API 키를 설정해주세요."

    try:
        # Prepare summary for Gemini
        summary = f"교육 과정 설문 분석:\n\n"

        for q in questions:
            # v2 필드를 우선하고 없으면 레거시 필드 폴백
            q_id = str(q.get('item_id') or q.get('questionId'))
            q_type = (q.get('metric_type') or q.get('type') or 'unknown').lower()
            q_text = q.get('item_text') or q.get('text') or '(제목없음)'

            summary += f"문항: {q_text}\n"
            summary += f"유형: {q_type}\n"

            # 통합 분류: v2와 레거시 타입을 모두 지원
            # likert, nps, rating → rating 카테고리
            if q_type in ['likert', 'nps', 'rating'] and q_id in all_analysis.get('rating', {}):
                data = all_analysis['rating'][q_id]
                if not data.get('no_data'):
                    summary += f"평균 점수: {data.get('average', 0):.2f}\n"
                    summary += f"응답 수: {data.get('total', 0)}\n"
                    summary += f"점수 분포: {data.get('counts', {})}\n"

            # single_choice, multi_choice, objective → objective 카테고리
            elif q_type in ['single_choice', 'multi_choice', 'objective'] and q_id in all_analysis.get('objective', {}):
                data = all_analysis['objective'][q_id]
                if not data.get('no_data'):
                    summary += f"선택 분포: {data.get('counts', {})}\n"
                    summary += f"응답 수: {data.get('total', 0)}\n"

            # text, subjective → subjective 카테고리
            elif q_type in ['text', 'subjective'] and q_id in all_analysis.get('subjective', {}):
                data = all_analysis['subjective'][q_id]
                if not data.get('no_data'):
                    summary += f"주관식 응답 수: {data.get('total', 0)}\n"
                    sample = data.get('responses', [])[:3]
                    if sample:
                        summary += f"샘플 응답: {', '.join(sample[:2])}\n"

            summary += "\n"

        prompt = f"""
{summary}

위 교육 설문 결과는 SK 임직원을 대상으로 한 교육 프로그램 효과 측정 및 만족도 조사 결과입니다.
분석의 목적은 교육 프로그램의 성과를 평가하고, **다음 과정 개선 사항**을 구체적으로 도출하는 것입니다.

이 교육 설문 결과를 분석하여 다음의 항목들을 **담당자 관점**에서 명확하게 제공해주세요:

1. **교육 프로그램 효과 요약 (KPI)**: 전반적인 만족도 및 이해도(likert/rating 문항)의 주요 결과 요약 및 성공/실패 여부 판단.
2. **주요 개선 필요 영역 (Deficiency Analysis)**: 평균 점수가 낮거나 부정적인 의견(주관식)이 집중된 **최대 2~3개의 영역(콘텐츠, 운영, 강사 등)**을 구체적인 근거와 함께 제시.
3. **실행 가능한 개선 제안 (Action Items)**: 다음 회차 교육을 위해 **담당자가 즉시 실행할 수 있는 구체적인 개선 제안 3가지 이상**을 작성.

한국어로 명확하고 구체적으로 작성해주세요.
        """

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt
        )

        return response.text

    except Exception as e:
        return f"AI 분석 중 오류가 발생했습니다: {str(e)}"


@st.cache_data(ttl=120)  # Cache for 2 minutes
def get_all_responses_cached(_spreadsheet, course_id: str):
    """
    [핵심 수정] 응답 시트 이름을 유연하게 찾아 v2 데이터를 로드하고, 디버깅 정보를 출력합니다.
    """
    target_sheet_name = None
    
    try:
        # 1. 시트 검색 (유연하게)
        worksheets = _spreadsheet.worksheets()
        for ws in worksheets:
            if "responses" in ws.title.lower() or "response" in ws.title.lower():
                target_sheet_name = ws.title
                break
        
        if not target_sheet_name:
            st.warning("🔍 Responses 시트를 찾을 수 없습니다. 시트 이름을 확인하세요.")
            return []

        # 2. 데이터 로드 (API 호출 지점)
        ws = _spreadsheet.worksheet(target_sheet_name)
        all_responses = ws.get_all_records()  # <-- 헤더를 기반으로 dict 리스트 로드
        
        # 3. course_id로 필터링
        filtered_responses = [
            r for r in all_responses 
            if str(r.get("course_id") or r.get("courseId")) == str(course_id)
        ]
        
        # 4. 🔑 최종 디버그 로직 추가: 로딩 상태를 명확히 표시
        st.caption(f"**🔍 로딩 디버그 (시트: {target_sheet_name})**")
        st.write(f"- 전체 응답 레코드 수: {len(all_responses)}")
        st.write(f"- 필터링 course_id: **{course_id}**")
        st.write(f"- 최종 필터링 후 응답 수: **{len(filtered_responses)}**개")
        
        if len(all_responses) > 0 and len(filtered_responses) == 0:
            st.error("🚨 필터링 실패! 시트에 course_id가 불일치할 수 있습니다.")
            # 실제 시트에 존재하는 course_id들을 보여줍니다.
            sheet_course_ids = set(str(r.get('course_id') or r.get('courseId')) for r in all_responses if r.get('course_id') or r.get('courseId'))
            st.code(f"시트 내 course_id 목록: {sheet_course_ids}")
        
        return filtered_responses
    
    except Exception as e:
        # API 오류가 아닌 다른 예외 처리
        st.error(f"v2 응답 데이터 로드 실패: {str(e)}")
        return []


@st.cache_data(ttl=120)  # Cache for 2 minutes
def get_all_questions_cached(_spreadsheet, course_id: str):
    """
    [최종 수정] Questions 시트 이름을 유연하게 찾아 데이터를 로드합니다.
    """
    target_sheet_name = None
    
    try:
        worksheets = _spreadsheet.worksheets()
        # 'questions' 또는 'question' 키워드가 포함된 시트를 찾습니다.
        for ws in worksheets:
            if "questions" in ws.title.lower() or "question" in ws.title.lower():
                target_sheet_name = ws.title
                break
        
        if not target_sheet_name:
            raise ValueError("스프레드시트에서 'Questions' 시트를 찾을 수 없습니다.")

        ws = _spreadsheet.worksheet(target_sheet_name)
        records = ws.get_all_records()
        filtered = [
            r for r in records if str(
                r.get("courseId")) == str(course_id)]
        # sort by order numeric if present
        try:
            filtered.sort(key=lambda r: int(str(r.get("order", "0") or 0)))
        except Exception:
            pass
        return filtered
    except Exception as e:
        st.error(f"문항 데이터 로드 실패: {str(e)}")
        if target_sheet_name:
            st.info(f"💡 마지막으로 시도한 시트 이름: {target_sheet_name}")
        else:
            st.info("💡 'Questions' 키워드를 포함하는 시트를 찾지 못했습니다.")
        return []


@st.cache_data(ttl=180)  # Cache for 3 minutes
def get_all_courses_cached(_spreadsheet):
    """
    [최종 수정] Courses 시트 이름을 유연하게 찾아 데이터를 로드합니다.
    """
    target_sheet_name = None
    
    try:
        worksheets = _spreadsheet.worksheets()
        # 'courses' 또는 'course' 키워드가 포함된 시트를 찾습니다.
        for ws in worksheets:
            if "courses" in ws.title.lower() or "course" in ws.title.lower():
                target_sheet_name = ws.title
                break
        
        if not target_sheet_name:
            raise ValueError("스프레드시트에서 'Courses' 시트를 찾을 수 없습니다.")

        ws = _spreadsheet.worksheet(target_sheet_name)
        return ws.get_all_records()
    except Exception as e:
        st.error(f"과정 데이터 로드 실패: {str(e)}")
        if target_sheet_name:
            st.info(f"💡 마지막으로 시도한 시트 이름: {target_sheet_name}")
        else:
            st.info("💡 'Courses' 키워드를 포함하는 시트를 찾지 못했습니다.")
        return []


@st.cache_data(ttl=120)  # Cache for 2 minutes
def get_course_items_cached(_spreadsheet, course_id: str):
    """
    [API 쿼터 해결] gsheets_utils.get_course_items 호출을 캐시하여 
    API Read 요청을 줄이고 쿼터 초과 에러를 방지합니다.
    """
    try:
        # gsheets_utils의 get_course_items 함수 호출 (이 함수는 API를 사용함)
        return get_course_items(_spreadsheet, course_id)
    except Exception as e:
        # 문항 로드 실패 시 레거시 함수로 폴백하여 안정성 확보
        st.warning(f"get_course_items 실패. 레거시 list_questions으로 폴백: {str(e)}")
        return list_questions(_spreadsheet, course_id)


def page_dashboard(spreadsheet, course_id: str):
    """Dashboard page for analyzing survey results (v2 스키마 호환)"""

    # 안전 조치: 모든 경로에서 all_course_responses 변수가 존재하도록 보장
    all_course_responses = []

    # v2 스키마 시도
    try:
        course_v2 = get_course_by_id_v2(spreadsheet, course_id)
        if course_v2:
            use_v2 = True
        else:
            # 레거시로 fallback
            course = get_course_by_id(spreadsheet, course_id)
            use_v2 = False
    except Exception as e:
        st.error(f"❌ 시트 로딩 실패: {str(e)}")
        st.info("💡 DB 설정 탭에서 스키마를 확인하고 v2 초기화를 실행하세요.")
        return

    # 과정 정보 확인
    if use_v2:
        if not course_v2:
            st.error(f"❌ 과정을 찾을 수 없습니다: {course_id}")
            st.info("💡 '과정 리스트' 탭에서 course_id를 확인하세요.")
            return
        course_title = f"{
    course_v2.get(
        'program_name',
        '')} - {
            course_v2.get(
                'theme',
                 '')}"
        course_desc = f"{
    course_v2.get(
        'event_type',
        '')} | {
            course_v2.get(
                'session_no',
                 '')}회차"
    else:
        if not course:
            st.error(f"❌ 과정을 찾을 수 없습니다: {course_id}")
        return
        course_title = course.get('title', '')
        course_desc = course.get('description', '')

    st.markdown(
        f"""
        <div class="sk-page-header">
          <div class="title">미래반도체 교육 과정 Dashboard</div>
          <div class="subtitle">{course_title} · {course_desc}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 문항 조회 (v2 스키마 우선)
    try:
        # ⚠️ get_course_items 대신 캐시된 헬퍼 함수 호출
        questions = get_course_items_cached(spreadsheet, course_id)
        
        if not questions:
            st.info("📝 설문 문항이 없습니다. '설문 편집'에서 문항을 추가하세요.")
            return
        
        # use_v2 플래그 설정 (questions에 item_id 필드가 있으면 v2로 간주)
        use_v2 = any(q.get('item_id') for q in questions)
        
    except Exception as e:
        st.error(f"❌ 문항 데이터 로딩 실패: {str(e)}")
        return
    
    # 응답 조회 (v2 스키마 우선)
    # ⚠️ 주의: get_responses_v2가 Responses_v2 시트가 없어 실패하는 문제를 우회합니다.
    try:
        # 1. 응답 데이터를 로드합니다 (시트 이름 문제 우회 로직 포함)
        all_course_responses = get_all_responses_cached(spreadsheet, course_id)
        
        # 2. 로드된 데이터를 분석에 사용할 responses 변수에 할당합니다.
        responses = all_course_responses
        
        # 3. 로드된 데이터를 바탕으로 use_v2 플래그를 재설정합니다.
        #    (Course 정보 조회 성공 여부와 관계없이 응답 데이터의 존재 여부를 우선 확인)
        if not all_course_responses:
            st.warning("⚠️ 아직 응답 데이터가 없습니다.")
            st.info("💡 '일반 사용자 모드'에서 설문에 응답하거나, '파일 업로드' 탭에서 응답 데이터를 적재하세요.")
            return

    except Exception as e:
        st.error(f"❌ 응답 데이터 로딩 중 치명적인 오류 발생: {str(e)}")
        return

    # use_v2 플래그는 course_v2 조회 시 결정된 값을 사용합니다 (로딩 로직과 분리)
    # all_course_responses 변수는 이제 분석 함수의 입력으로 사용됩니다.
    
    # KPI Summary - SVG 아이콘 사용
    st.markdown('''
        <h3>
            <svg class="icon-svg icon-chip" viewBox="0 0 24 24">
                <rect x="3" y="3" width="18" height="18" rx="2"/>
                <line x1="7" y1="3" x2="7" y2="0"/>
                <line x1="12" y1="3" x2="12" y2="0"/>
                <line x1="17" y1="3" x2="17" y2="0"/>
                <line x1="7" y1="24" x2="7" y2="21"/>
                <line x1="12" y1="24" x2="12" y2="21"/>
                <line x1="17" y1="24" x2="17" y2="21"/>
                <line x1="0" y1="7" x2="3" y2="7"/>
                <line x1="0" y1="17" x2="3" y2="17"/>
                <line x1="21" y1="7" x2="24" y2="7"/>
                <line x1="21" y1="17" x2="24" y2="17"/>
                <circle cx="12" cy="12" r="3"/>
            </svg>
            주요 지표
        </h3>
    ''', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    
    try:
        # 안전하게 respondentHash 추출 (v2와 레거시 호환)
        respondent_hashes = set()
        for r in responses:
            hash_val = r.get('respondentHash') or r.get('respondent_id')
            if hash_val:
                respondent_hashes.add(str(hash_val))
        
        unique_respondents = len(respondent_hashes)
        total_questions = len(questions)
        
        # 📈 핵심 KPI: 평점형 문항의 전체 평균 계산
        rating_qs = [q for q in questions if (q.get('metric_type') or q.get('type') or '').lower() in ['rating', 'likert', 'nps']]
        overall_satisfaction = 0.0
        if rating_qs:
            total_avg = 0.0
            valid_count = 0
            for q in rating_qs:
                data = analyze_rating_data(spreadsheet, course_id, q, all_course_responses)
                if not data.get('no_data') and data.get('average', 0) > 0:
                    total_avg += data.get('average', 0)
                    valid_count += 1
            if valid_count > 0:
                overall_satisfaction = total_avg / valid_count
        
        with col1:
            st.metric("총 응답자 수", unique_respondents)
        with col2:
            st.metric("총 문항 수", total_questions)
        with col3:
            # 💡 교육 담당자를 위한 핵심 KPI
            if overall_satisfaction > 0:
                st.metric("평균 만족도", f"{overall_satisfaction:.2f}점", 
                         delta="우수" if overall_satisfaction >= 4.0 else "개선 필요")
            else:
                st.metric("평균 만족도", "N/A")
    except Exception as e:
        st.warning("⚠️ 주요 지표를 계산하는 중 오류가 발생했습니다.")
        with st.expander("🔍 상세 정보"):
            st.code(str(e))
    
    st.divider()
    
    # Tabs for different analyses
    tab1, tab2, tab3, tab4 = st.tabs([
        "객관식", 
        "평점", 
        "주관식", 
        "AI 인사이트"
    ])
    
    all_analysis = {
        'objective': {},
        'rating': {},
        'subjective': {}
    }
    
    # 💡 헬퍼 함수: v2의 metric_type을 우선하고 없으면 레거시 type을 반환
    def get_q_type(q):
        """v2의 metric_type을 우선하고 없으면 레거시 type을 반환"""
        return (q.get('metric_type') or q.get('type') or 'unknown').lower()
    
    def get_q_text(q):
        """v2의 item_text를 우선하고 없으면 레거시 text를 반환"""
        return q.get('item_text') or q.get('text') or '(제목없음)'
    
    def get_q_id(q):
        """v2의 item_id를 우선하고 없으면 레거시 questionId를 반환"""
        return str(q.get('item_id') or q.get('questionId'))
    
    def deduplicate_questions(question_list: List[Dict]) -> List[Dict]:
        seen_ids = set()
        unique_list = []
        for q in question_list:
            q_id = get_q_id(q)
            if q_id in seen_ids:
                continue
            seen_ids.add(q_id)
            unique_list.append(q)
        return unique_list

    # 응답을 item_id 기준으로 그룹화
    responses_by_item = defaultdict(list)
    for resp in all_course_responses:
        resp_key = str(resp.get("item_id") or resp.get("questionId"))
        if resp_key:
            responses_by_item[resp_key].append(resp)

    unique_question_ids = {get_q_id(q) for q in questions if get_q_id(q)}

    with st.expander("데이터 상태 요약", expanded=False):
        st.write("- 총 문항 수 (Course_Item_Map):", len(questions))
        st.write("- 고유 문항 수:", len(unique_question_ids))
        st.write("- 응답 레코드 수:", len(all_course_responses))
        st.write("- 응답이 있는 문항 수:", len({k for k, v in responses_by_item.items() if v}))

    with tab1:
        st.markdown('''
            <h3>
                <svg class="icon-svg icon-chart" viewBox="0 0 24 24">
                    <rect x="3" y="8" width="4" height="13"/>
                    <rect x="10" y="4" width="4" height="17"/>
                    <rect x="17" y="11" width="4" height="10"/>
                    <line x1="0" y1="23" x2="24" y2="23"/>
                </svg>
                객관식 문항 분석
            </h3>
        ''', unsafe_allow_html=True)
        
        # v2 metric_type을 우선 사용한 통합 분류
        objective_qs = [q for q in questions if get_q_type(q) in ['objective', 'single_choice', 'multi_choice']]
        objective_qs = deduplicate_questions(objective_qs)
        
        # 💡 경품/개인정보 관련 문항 필터링
        exclude_keywords = ["경품", "개인정보", "동의", "수집", "이용", "제공", "consent", "privacy", "prize"]
        objective_qs = [q for q in objective_qs 
                       if not any(keyword in get_q_text(q).lower() for keyword in exclude_keywords)]
        
        if not objective_qs:
            st.info("객관식 문항이 없습니다.")
        else:
            for q in objective_qs:
                q_text = get_q_text(q)
                q_id = get_q_id(q)
                related_responses = responses_by_item.get(q_id, [])

                st.markdown(f"#### {q_text}")

                if not related_responses:
                    st.info("응답 데이터가 없습니다. (0건)")
                    st.divider()
                    continue

                data = analyze_objective_data(spreadsheet, course_id, q, all_course_responses)
                all_analysis['objective'][q_id] = data

                if data.get('no_data'):
                    st.info("응답 데이터가 없습니다.")
                else:
                    st.caption(f"응답 수: {len(related_responses)}")
                    df = pd.DataFrame(list(data['counts'].items()), columns=['선택지', '응답 수'])
                    fig = px.bar(df, x='선택지', y='응답 수', title=f"총 응답: {data['total']}")
                    st.plotly_chart(fig, use_container_width=True, key=f"obj_chart_{q_id}")

                st.divider()
    
    with tab2:
        st.markdown('''
            <h3>
                <svg class="icon-svg icon-star" viewBox="0 0 24 24">
                    <polygon points="12,2 15,9 22,10 17,15 18,22 12,18 6,22 7,15 2,10 9,9"/>
                </svg>
                평점형 문항 분석
            </h3>
        ''', unsafe_allow_html=True)
        
        # v2 metric_type을 우선 사용한 통합 분류
        rating_qs = [q for q in questions if get_q_type(q) in ['rating', 'likert', 'nps']]
        rating_qs = deduplicate_questions(rating_qs)
        
        if not rating_qs:
            st.info("평점형 문항이 없습니다.")
        else:
            for q in rating_qs:
                q_text = get_q_text(q)
                q_id = get_q_id(q)

                related_responses = responses_by_item.get(q_id, [])

                st.markdown(f"#### {q_text}")

                if not related_responses:
                    st.info("응답 데이터가 없습니다. (0건)")
                    st.divider()
                    continue

                data = analyze_rating_data(spreadsheet, course_id, q, all_course_responses)
                all_analysis['rating'][q_id] = data
                
                if data.get('no_data'):
                    st.info("응답 데이터가 없습니다.")
                else:
                    st.caption(f"응답 수: {len(related_responses)}")
                    col_a, col_b = st.columns([2, 1])
                    
                    with col_a:
                        df = pd.DataFrame(list(data['counts'].items()), columns=['평점', '응답 수'])
                        df['평점'] = df['평점'].astype(str) + '점'
                        fig = px.pie(df, names='평점', values='응답 수', title=f"평점 분포 (평균: {data['average']:.2f}점)")
                        st.plotly_chart(fig, use_container_width=True, key=f"rating_chart_{q_id}")
                    
                    with col_b:
                        st.metric("평균 평점", f"{data['average']:.2f}점")
                        st.metric("총 응답 수", data['total'])

                st.divider()
    
    with tab3:
        st.markdown('''
            <h3>
                <svg class="icon-svg icon-message" viewBox="0 0 24 24">
                    <path d="M21,3 L3,3 C1.9,3 1,3.9 1,5 L1,17 C1,18.1 1.9,19 3,19 L7,19 L12,23 L17,19 L21,19 C22.1,19 23,18.1 23,17 L23,5 C23,3.9 22.1,3 21,3 Z"/>
                    <line x1="6" y1="9" x2="18" y2="9"/>
                    <line x1="6" y1="13" x2="15" y2="13"/>
                </svg>
                주관식 문항 분석
            </h3>
        ''', unsafe_allow_html=True)
        
        # v2 metric_type을 우선 사용한 통합 분류
        subjective_qs = [q for q in questions if get_q_type(q) in ['subjective', 'text']]
        subjective_qs = deduplicate_questions(subjective_qs)
        
        if not subjective_qs:
            st.info("주관식 문항이 없습니다.")
        else:
            for q in subjective_qs:
                q_text = get_q_text(q)
                q_id = get_q_id(q)
                related_responses = responses_by_item.get(q_id, [])
                
                st.markdown(f"#### {q_text}")
                if not related_responses:
                    st.info("응답 데이터가 없습니다. (0건)")
                    st.divider()
                    continue

                data = analyze_subjective_data(spreadsheet, course_id, q, all_course_responses)
                all_analysis['subjective'][q_id] = data
                
                if data.get('no_data'):
                    st.info("응답 데이터가 없습니다.")
                else:
                    texts = data['responses']
                    st.caption(f"응답 수: {len(texts)}")
                    
                    if len(texts) >= 5:
                        # Wordcloud
                        st.markdown("##### 워드 클라우드")
                        fig = generate_wordcloud(texts)
                        if fig:
                            st.pyplot(fig)
                    
                    # Show responses
                    st.markdown(f"##### 전체 응답 ({len(texts)}개)")
                    with st.expander("응답 보기"):
                        for idx, text in enumerate(texts, 1):
                            st.markdown(f"{idx}. {text}")
                
                st.divider()
    
    with tab4:
        st.markdown('''
            <h3>
                <svg class="icon-svg icon-brain" viewBox="0 0 24 24">
                    <path d="M12,2 C8,2 5,5 5,9 L5,15 C5,19 8,22 12,22 C16,22 19,19 19,15 L19,9 C19,5 16,2 12,2 Z"/>
                    <circle cx="9" cy="10" r="1.5"/>
                    <circle cx="15" cy="10" r="1.5"/>
                    <path d="M9,14 Q12,16 15,14"/>
                    <path d="M7,9 L7,6 M17,9 L17,6 M12,2 L12,5"/>
                </svg>
                AI 기반 인사이트
            </h3>
        ''', unsafe_allow_html=True)
        
        if st.button("AI 분석 실행", type="primary"):
            with st.spinner("Gemini AI로 분석 중..."):
                # v2와 레거시 통합 호환
                insights = generate_ai_insights(spreadsheet, course_id, questions, all_analysis)
                
                st.markdown("#### 분석 결과")
                st.markdown(insights)
                
                # Save to Analysis sheet (v2는 Insights 시트 사용 가능)
                if use_v2:
                    # v2 스키마: Insights 시트에 저장
                    try:
                        insight_data = {
                            "insight_id": f"INS-{int(datetime.now(timezone.utc).timestamp())}",
                            "course_id": course_id,
                            "insight_type": "ai_generated",
                            "insight_text": insights,
                            "dimension": "overall",
                            "sentiment_score": None,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "metadata": json.dumps({
                                "objective": all_analysis['objective'],
                                "rating": all_analysis['rating'],
                                "subjective_count": {k: v.get('total', 0) for k, v in all_analysis['subjective'].items()}
                            }, ensure_ascii=False)
                        }
                        save_insight(spreadsheet, insight_data)
                        st.success("✅ 분석 결과가 Insights 시트에 저장되었습니다.")
                    except Exception as e:
                        st.warning(f"⚠️ Insights 저장 실패: {str(e)}")
                else:
                    # 레거시 스키마: Analysis 시트에 저장
                    analysis_data = {
                        "objectiveJson": json.dumps(all_analysis['objective'], ensure_ascii=False),
                        "ratingJson": json.dumps(all_analysis['rating'], ensure_ascii=False),
                        "subjectiveJson": json.dumps(all_analysis['subjective'], ensure_ascii=False),
                        "insightsText": insights,
                        "actionItemsText": "",
                        "confidence": "0.85"
                    }
                    save_analysis(spreadsheet, course_id, analysis_data)
                    st.success("✅ 분석 결과가 Analysis 시트에 저장되었습니다.")


def main():
    set_page_config()
    apply_global_styles()
    
    # 파스텔 배경 장식 추가
    st.markdown(
        """
        <div class="ai-bg-decoration"></div>
        <style>
          /* 선형 패턴 장식 */
          .stApp::before {
            content: '';
            position: fixed;
            top: 10%;
            right: 5%;
            width: 150px;
            height: 150px;
            background-image: 
              repeating-linear-gradient(45deg, transparent, transparent 15px, rgba(168, 216, 234, 0.1) 15px, rgba(168, 216, 234, 0.1) 30px),
              repeating-linear-gradient(-45deg, transparent, transparent 15px, rgba(212, 165, 216, 0.1) 15px, rgba(212, 165, 216, 0.1) 30px);
            border-radius: 50%;
            z-index: 0;
            pointer-events: none;
            animation: pattern-rotate 30s linear infinite;
          }
          
          .stApp::after {
            content: '';
            position: fixed;
            bottom: 15%;
            left: 8%;
            width: 100px;
            height: 100px;
            border: 3px solid var(--pastel-mint);
            border-radius: 50%;
            z-index: 0;
            pointer-events: none;
            animation: float 8s ease-in-out infinite;
            opacity: 0.3;
          }
          
          @keyframes pattern-rotate {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
          }
          
          @keyframes float {
            0%, 100% { transform: translateY(0px) scale(1); }
            50% { transform: translateY(-25px) scale(1.05); }
          }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    spreadsheet = require_spreadsheet()
    
    # 사이드바에서 모드 선택
    is_admin_mode = sidebar_mode_selector()
    authenticate_if_needed(is_admin_mode)

    is_admin = is_admin_mode and bool(st.session_state.get("admin_authenticated"))

    if is_admin:
        st.markdown(f"- **{ADMIN_BADGE}**: 세션 만료 30분")
        
        # Cache control in sidebar
        with st.sidebar:
            st.divider()
            st.markdown("### 캐시 관리")
            st.caption("데이터가 업데이트되지 않을 때 캐시를 클리어하세요.")
            if st.button("캐시 클리어", help="모든 캐시된 데이터를 새로고침합니다"):
                st.cache_data.clear()
                st.success("캐시가 클리어되었습니다!")
                st.info("페이지를 새로고침하면 최신 데이터가 로드됩니다.")
        
        tab1, tab2, tab3 = st.tabs(["과정 리스트", "DB 설정", "파일 업로드"])
        with tab1:
            page_course_list(spreadsheet, is_admin=True)
        with tab2:
            page_setup_db(spreadsheet)
        with tab3:
            page_upload_files(spreadsheet)
    else:
        # 일반 사용자 모드: 설문 참여
        page_survey_participation(spreadsheet)


if __name__ == "__main__":
    main()


