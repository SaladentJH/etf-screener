"""
ETF 종목별 투자자 순매수 수집기
매 영업일 장 마감 후 실행 (18:00 KST)
KIS API FHKST01010900 사용 → 최근 30일치 조회
오늘 날짜 데이터만 추출해서 누적 저장
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


# ── 설정 ────────────────────────────────────────────
KIS_APP_KEY    = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_BASE_URL   = "https://openapi.koreainvestment.com:9443"
DATA_FILE      = "etf_investor_daily.csv"  # GitHub에 누적 저장


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def kst_now():
    return datetime.utcnow() + timedelta(hours=9)


def get_recent_business_day():
    dt = kst_now()
    # 오늘이 영업일이면 오늘, 아니면 직전 영업일
    while dt.weekday() >= 5:
        dt -= timedelta(days=1)
    return dt.strftime("%Y%m%d")


def get_kis_token():
    r = requests.post(
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={
            "grant_type": "client_credentials",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
        },
        timeout=10,
    )
    token = r.json().get("access_token", "")
    assert token, f"토큰 발급 실패: {r.json()}"
    return token


def get_etf_investor(ticker, token):
    """ETF 종목별 투자자 순매수 조회 (최근 30일치)"""
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010900",
    }
    try:
        r = requests.get(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor",
            headers=headers,
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            timeout=10,
        )
        data = r.json()
        if data.get("rt_cd") != "0":
            return []
        rows = []
        for row in data.get("output", []):
            try:
                rows.append({
                    "date":   row["stck_bsop_date"],
                    "ticker": ticker,
                    "prsn":   float(row.get("prsn_ntby_tr_pbmn", 0) or 0) * 1_000_000,
                    "frgn":   float(row.get("frgn_ntby_tr_pbmn", 0) or 0) * 1_000_000,
                    "orgn":   float(row.get("orgn_ntby_tr_pbmn", 0) or 0) * 1_000_000,
                })
            except:
                pass
        return rows
    except Exception as e:
        log(f"  오류 ({ticker}): {e}")
        return []


def get_etf_list():
    """pykrx로 전체 ETF 목록 가져오기"""
    try:
        from pykrx import stock
        today = kst_now().strftime("%Y%m%d")
        return stock.get_etf_ticker_list(date=today)
    except Exception as e:
        log(f"ETF 목록 조회 실패: {e}")
        return []


def load_existing_data():
    """기존 누적 데이터 로드"""
    if os.path.exists(DATA_FILE):
        df = pd.read_csv(DATA_FILE, dtype={"ticker": str, "date": str})
        log(f"기존 데이터 로드: {len(df):,}행")
        return df
    return pd.DataFrame(columns=["date", "ticker", "prsn", "frgn", "orgn"])


def main():
    assert KIS_APP_KEY,    "KIS_APP_KEY 없음"
    assert KIS_APP_SECRET, "KIS_APP_SECRET 없음"

    target_date = get_recent_business_day()
    log(f"수집 대상 날짜: {target_date}")

    # 기존 데이터 로드
    existing = load_existing_data()

    # 오늘 날짜 이미 수집됐는지 확인
    if not existing.empty and target_date in existing["date"].values:
        already = existing[existing["date"] == target_date]["ticker"].nunique()
        log(f"오늘({target_date}) 이미 {already}개 ETF 수집됨 → 스킵")
        return

    # KIS 토큰 발급
    log("KIS 토큰 발급 중...")
    token = get_kis_token()
    log("토큰 발급 완료")

    # ETF 목록
    log("ETF 목록 수집 중...")
    etf_list = get_etf_list()
    log(f"ETF 수: {len(etf_list)}개")

    if not etf_list:
        log("ETF 목록 없음 → 종료")
        return

    # 수집
    log(f"투자자 데이터 수집 시작 ({len(etf_list)}개 ETF)...")
    today_rows = []
    failed = 0

    for i, ticker in enumerate(etf_list):
        rows = get_etf_investor(ticker, token)

        # 오늘 날짜 데이터만 추출
        today_data = [r for r in rows if r["date"] == target_date]
        today_rows.extend(today_data)

        if (i + 1) % 5 == 0:
            time.sleep(1.1)  # KIS API 5회/초 제한
        if (i + 1) % 200 == 0:
            log(f"  {i+1}/{len(etf_list)} 처리 완료, 오늘 데이터: {len(today_rows)}개")

    log(f"수집 완료: {len(today_rows)}개 ETF 오늘 데이터")

    if not today_rows:
        log("오늘 데이터 없음 (휴장일일 수 있음)")
        return

    # 새 데이터 추가
    new_df = pd.DataFrame(today_rows)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
    combined = combined.sort_values(["date", "ticker"]).reset_index(drop=True)

    # 저장 (최근 2년치만 유지)
    cutoff = (kst_now() - timedelta(days=730)).strftime("%Y%m%d")
    combined = combined[combined["date"] >= cutoff]

    combined.to_csv(DATA_FILE, index=False)
    log(f"저장 완료: {DATA_FILE} ({len(combined):,}행)")
    log(f"날짜 범위: {combined['date'].min()} ~ {combined['date'].max()}")
    log(f"ETF 수: {combined['ticker'].nunique()}개")


if __name__ == "__main__":
    main()
