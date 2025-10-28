import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import folium
from streamlit_folium import folium_static
import os
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Streamlit Secrets 사용으로 .env 로드는 더 이상 필요하지 않습니다

# 페이지 설정
st.set_page_config(
    page_title="근태 관리 대시보드",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS 스타일링
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .summary-box {
        background-color: #e8f4fd;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# 메인 헤더
st.markdown('<h1 class="main-header">📊 근태 관리 대시보드</h1>', unsafe_allow_html=True)

# 수송스퀘어 위치 지도
st.markdown("## 🗺️ 수송스퀘어 위치")
# 수송스퀘어 좌표 (서울특별시 종로구 율곡로2길 19)
susong_lat = 37.5744
susong_lon = 126.9811

# Folium 지도 생성
m = folium.Map(
    location=[susong_lat, susong_lon], 
    zoom_start=16,
    tiles='OpenStreetMap'
)

# 수송스퀘어 마커 추가
folium.Marker(
    [susong_lat, susong_lon],
    popup='수송스퀘어<br>서울특별시 종로구 율곡로2길 19',
    tooltip='수송스퀘어',
    icon=folium.Icon(color='blue', icon='building', prefix='fa')
).add_to(m)

# 지도 표시
folium_static(m, width=800, height=400)

st.markdown("---")

# 데이터 처리 함수들
def process_attendance_data(df):
    """근태 데이터를 처리하고 정리하는 함수"""
    # 날짜 컬럼을 datetime으로 변환
    df['날짜'] = pd.to_datetime(df['날짜'])
    
    # 시간 컬럼들을 datetime으로 변환
    df['출근'] = pd.to_datetime(df['날짜'].dt.date.astype(str) + ' ' + df['출근'].astype(str))
    df['퇴근'] = pd.to_datetime(df['날짜'].dt.date.astype(str) + ' ' + df['퇴근'].astype(str))
    
    # 요일 추가
    df['요일'] = df['날짜'].dt.day_name()
    df['월'] = df['날짜'].dt.month
    df['주차'] = df['날짜'].dt.isocalendar().week
    
    return df

def calculate_summary_stats(df):
    """핵심 통계를 계산하는 함수"""
    stats = {
        '총_직원수': df['이름'].nunique(),
        '총_근무일수': df['날짜'].nunique(),
        '평균_근무시간': df['총 근무시간'].mean(),
        '평균_초과근무시간': df['초과근무시간'].mean(),
        '최대_근무시간': df['총 근무시간'].max(),
        '최소_근무시간': df['총 근무시간'].min(),
        '초과근무_비율': (df['초과근무시간'] > 0).mean() * 100
    }
    return stats

def generate_ai_insights(df, stats):
    """Gemini API를 사용하여 근태 데이터 인사이트를 생성하는 함수"""
    try:
        # API 키 확인 (Streamlit secrets 사용)
        api_key = st.secrets.get('GEMINI_API_KEY')
        st.write(f"🔍 API 키 확인: {'설정됨' if api_key else '설정되지 않음'}")
        if not api_key:
            return "⚠️ Gemini API 키가 설정되지 않았습니다. Streamlit secrets에 GEMINI_API_KEY를 설정해주세요."
        
        # 클라이언트 초기화
        client = genai.Client(api_key=api_key)
        
        # 데이터 요약 생성
        data_summary = f"""
        근태 데이터 분석 결과:
        - 총 직원 수: {stats['총_직원수']}명
        - 총 근무일 수: {stats['총_근무일수']}일
        - 평균 근무시간: {stats['평균_근무시간']:.1f}시간
        - 평균 초과근무시간: {stats['평균_초과근무시간']:.1f}시간
        - 초과근무 비율: {stats['초과근무_비율']:.1f}%
        - 최대 근무시간: {stats['최대_근무시간']}시간
        - 최소 근무시간: {stats['최소_근무시간']}시간
        
        부서별 통계:
        {df.groupby('부서')['총 근무시간'].agg(['mean', 'count']).round(1).to_string()}
        
        요일별 평균 근무시간:
        {df.groupby('요일')['총 근무시간'].mean().round(1).to_string()}
        """
        
        # 프롬프트 생성
        prompt = f"""
        다음은 회사의 근태 관리 데이터 분석 결과입니다. 
        이 데이터를 바탕으로 인사담당자에게 유용한 인사이트와 개선 제안사항을 제공해주세요.
        
        {data_summary}
        
        다음 항목들을 포함해서 분석해주세요:
        1. 근무 패턴의 특징과 문제점
        2. 부서별 근무 시간 차이 분석
        3. 초과근무 현황과 개선 방안
        4. 직원 워라밸 개선 제안
        5. 근태 관리 시스템 개선 제안
        
        한국어로 답변하고, 구체적이고 실행 가능한 제안사항을 포함해주세요.
        """
        
        # Gemini API 호출
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.7,
                max_output_tokens=10000,
                thinking_config=types.ThinkingConfig(
                    thinking_budget=-1
                )
            )
        )
        
        return response.text
        
    except Exception as e:
        return f"❌ AI 인사이트 생성 중 오류가 발생했습니다: {str(e)}"

# 사이드바 - 파일 업로드
with st.sidebar:
    st.header("📁 데이터 업로드")
    uploaded_file = st.file_uploader(
        "엑셀 파일을 업로드하세요",
        type=['xlsx', 'xls'],
        help="근태파일 시트가 포함된 엑셀 파일을 업로드하세요"
    )
    
    if uploaded_file is not None:
        try:
            # 엑셀 파일에서 '근태파일' 시트 읽기
            df = pd.read_excel(uploaded_file, sheet_name='근태파일')
            st.success("파일이 성공적으로 업로드되었습니다!")
            st.write(f"데이터 행 수: {len(df)}")
        except Exception as e:
            st.error(f"파일을 읽는 중 오류가 발생했습니다: {str(e)}")
            df = None
    else:
        # 샘플 데이터 생성
        st.info("샘플 데이터를 사용합니다")
        sample_data = {
            '이름': ['김하늘', '정하늘', '김민준', '윤서연', '김지우', '최도윤', '최민준'] * 5,
            '부서': ['운영팀', '기획팀', '운영팀', '마케팅팀', '인사팀', '재무팀', '운영팀'] * 5,
            '날짜': pd.date_range('2024-03-01', periods=35, freq='D').strftime('%Y-%m-%d').tolist(),
            '출근': ['10:00', '09:00', '08:00', '10:00', '09:00', '09:00', '08:00'] * 5,
            '퇴근': ['20:00', '17:00', '16:00', '18:00', '20:00', '18:00', '19:00'] * 5,
            '총 근무시간': [10, 8, 8, 8, 11, 9, 11] * 5,
            '초과근무시간': [1, 0, 0, 0, 2, 0, 2] * 5
        }
        df = pd.DataFrame(sample_data)

# 메인 대시보드
if df is not None:
    # 데이터 처리
    df_processed = process_attendance_data(df)
    
    # 핵심 통계 계산
    stats = calculate_summary_stats(df_processed)
    
    # 핵심 요약 통계 표시
    st.markdown("## 📈 핵심 요약")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="총 직원 수",
            value=f"{stats['총_직원수']}명",
            delta=None
        )
    
    with col2:
        st.metric(
            label="평균 근무시간",
            value=f"{stats['평균_근무시간']:.1f}시간",
            delta=None
        )
    
    with col3:
        st.metric(
            label="평균 초과근무",
            value=f"{stats['평균_초과근무시간']:.1f}시간",
            delta=None
        )
    
    with col4:
        st.metric(
            label="초과근무 비율",
            value=f"{stats['초과근무_비율']:.1f}%",
            delta=None
        )
    
    # 상세 통계 박스
    st.markdown('<div class="summary-box">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write(f"**총 근무일수:** {stats['총_근무일수']}일")
        st.write(f"**최대 근무시간:** {stats['최대_근무시간']}시간")
    
    with col2:
        st.write(f"**최소 근무시간:** {stats['최소_근무시간']}시간")
        st.write(f"**데이터 기간:** {df_processed['날짜'].min().strftime('%Y-%m-%d')} ~ {df_processed['날짜'].max().strftime('%Y-%m-%d')}")
    
    with col3:
        st.write(f"**부서 수:** {df_processed['부서'].nunique()}개")
        st.write(f"**부서별 평균 근무시간:** {df_processed.groupby('부서')['총 근무시간'].mean().round(1).to_dict()}")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # 차트 섹션
    st.markdown("## 📊 차트 분석")
    
    # 탭으로 차트 구분
    tab1, tab2, tab3, tab4 = st.tabs(["📅 시간별 분석", "👥 부서별 분석", "👤 개인별 분석", "📈 추이 분석"])
    
    with tab1:
        # 시간별 근무시간 분포
        fig1 = px.histogram(
            df_processed, 
            x='총 근무시간', 
            title='근무시간 분포',
            nbins=20,
            color_discrete_sequence=['#1f77b4']
        )
        fig1.update_layout(showlegend=False)
        st.plotly_chart(fig1, use_container_width=True)
        
        # 요일별 평균 근무시간
        daily_avg = df_processed.groupby('요일')['총 근무시간'].mean().reindex(
            ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        )
        
        fig2 = px.bar(
            x=daily_avg.index,
            y=daily_avg.values,
            title='요일별 평균 근무시간',
            color=daily_avg.values,
            color_continuous_scale='Blues'
        )
        fig2.update_layout(showlegend=False, xaxis_title="요일", yaxis_title="평균 근무시간(시간)")
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab2:
        # 부서별 평균 근무시간
        dept_stats = df_processed.groupby('부서').agg({
            '총 근무시간': 'mean',
            '초과근무시간': 'mean',
            '이름': 'nunique'
        }).round(1)
        dept_stats.columns = ['평균 근무시간', '평균 초과근무', '직원수']
        
        fig3 = px.bar(
            dept_stats,
            x=dept_stats.index,
            y='평균 근무시간',
            title='부서별 평균 근무시간',
            color='평균 근무시간',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig3, use_container_width=True)
        
        # 부서별 초과근무 비율
        dept_overtime = df_processed.groupby('부서').apply(
            lambda x: (x['초과근무시간'] > 0).mean() * 100
        ).round(1)
        
        fig4 = px.pie(
            values=dept_overtime.values,
            names=dept_overtime.index,
            title='부서별 초과근무 비율'
        )
        st.plotly_chart(fig4, use_container_width=True)
    
    with tab3:
        # 개인별 근무시간 랭킹
        person_stats = df_processed.groupby('이름').agg({
            '총 근무시간': ['mean', 'sum', 'count'],
            '초과근무시간': 'sum'
        }).round(1)
        person_stats.columns = ['평균 근무시간', '총 근무시간', '근무일수', '총 초과근무']
        person_stats = person_stats.sort_values('평균 근무시간', ascending=False)
        
        fig5 = px.bar(
            person_stats.head(10),
            x=person_stats.head(10).index,
            y='평균 근무시간',
            title='개인별 평균 근무시간 TOP 10',
            color='평균 근무시간',
            color_continuous_scale='Reds'
        )
        fig5.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig5, use_container_width=True)
        
        # 개인별 초과근무 시간
        fig6 = px.bar(
            person_stats.head(10),
            x=person_stats.head(10).index,
            y='총 초과근무',
            title='개인별 총 초과근무 시간 TOP 10',
            color='총 초과근무',
            color_continuous_scale='Oranges'
        )
        fig6.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig6, use_container_width=True)
    
    with tab4:
        # 일별 평균 근무시간 추이
        daily_trend = df_processed.groupby('날짜')['총 근무시간'].mean().reset_index()
        
        fig7 = px.line(
            daily_trend,
            x='날짜',
            y='총 근무시간',
            title='일별 평균 근무시간 추이',
            markers=True
        )
        fig7.update_layout(xaxis_title="날짜", yaxis_title="평균 근무시간(시간)")
        st.plotly_chart(fig7, use_container_width=True)
        
        # 주차별 평균 근무시간
        weekly_trend = df_processed.groupby('주차')['총 근무시간'].mean().reset_index()
        
        fig8 = px.bar(
            weekly_trend,
            x='주차',
            y='총 근무시간',
            title='주차별 평균 근무시간',
            color='총 근무시간',
            color_continuous_scale='Greens'
        )
        st.plotly_chart(fig8, use_container_width=True)
    
    # 데이터 테이블
    st.markdown("## 📋 상세 데이터")
    
    # 필터링 옵션
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_departments = st.multiselect(
            "부서 선택",
            options=df_processed['부서'].unique(),
            default=df_processed['부서'].unique()
        )
    
    with col2:
        selected_names = st.multiselect(
            "직원 선택",
            options=df_processed['이름'].unique(),
            default=df_processed['이름'].unique()
        )
    
    with col3:
        date_range = st.date_input(
            "날짜 범위",
            value=(df_processed['날짜'].min().date(), df_processed['날짜'].max().date()),
            min_value=df_processed['날짜'].min().date(),
            max_value=df_processed['날짜'].max().date()
        )
    
    # 필터링된 데이터
    filtered_df = df_processed[
        (df_processed['부서'].isin(selected_departments)) &
        (df_processed['이름'].isin(selected_names)) &
        (df_processed['날짜'].dt.date >= date_range[0]) &
        (df_processed['날짜'].dt.date <= date_range[1])
    ]
    
    st.dataframe(
        filtered_df[['이름', '부서', '날짜', '출근', '퇴근', '총 근무시간', '초과근무시간', '요일']],
        use_container_width=True
    )
    
    # 다운로드 버튼
    csv = filtered_df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(
        label="필터링된 데이터 다운로드 (CSV)",
        data=csv,
        file_name=f"근태데이터_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
    
    # AI 인사이트 섹션
    st.markdown("---")
    st.markdown("## 🤖 AI 인사이트 분석")
    
    # AI 인사이트 생성 버튼
    if st.button("🔍 AI 인사이트 생성", type="primary"):
        with st.spinner("AI가 데이터를 분석하고 인사이트를 생성하고 있습니다..."):
            insights = generate_ai_insights(df_processed, stats)
            
            # 인사이트 표시
            st.markdown("### 📊 분석 결과")
            st.markdown(insights)
            
            # 인사이트 다운로드 버튼
            st.download_button(
                label="📄 AI 인사이트 다운로드 (TXT)",
                data=insights,
                file_name=f"AI_인사이트_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                mime="text/plain"
            )
    
    # API 키 설정 안내 (Streamlit secrets 기준)
    if not st.secrets.get('GEMINI_API_KEY'):
        st.warning("""
        ⚠️ **Gemini API 키가 설정되지 않았습니다.**
        
        AI 인사이트 기능을 사용하려면:
        1. [Google AI Studio](https://ai.google.dev/)에서 API 키를 발급받으세요
        2. Streamlit secrets에 `GEMINI_API_KEY`를 설정하세요
           - 로컬: `.streamlit/secrets.toml` 파일에 아래와 같이 추가
             
             [secrets]
             GEMINI_API_KEY = "your_api_key_here"
           
           - 클라우드(share.streamlit.io): 앱 대시보드의 Secrets에 `GEMINI_API_KEY` 추가
        
        자세한 설정 방법은 `env.txt` 파일을 참고하세요.
        """)

else:
    st.info("👈 왼쪽 사이드바에서 엑셀 파일을 업로드하거나 샘플 데이터를 사용하세요.")

