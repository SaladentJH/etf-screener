"""
ETF 수급 기반 종목 스크리너
미래에셋증권 "신(新) 수급의 시대" 전략 구현
pykrx-openapi KRXOpenAPI 클래스 사용
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from pykrx_openapi import KRXOpenAPI

# ─── 설정 ────────────────────────────────────────────
KRX_API_KEY        = os.environ.get("KRX_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "@saladentnews")

MIN_ETF_INFLOW  = 10_000_000_000   # 최소 거래대금 10억
TOP_N           = 20
LOOKBACK_DAYS   = 7                # 1주일 (영업일 5일)

EXCLUDE_KEYWORDS = [
    "레버리지", "인버스", "2X", "3X", "-1X", "곱버스",
    "해외", "미국", "중국", "일본", "인도", "베트남",
    "나스닥", "S&P", "채권", "국채", "달러", "금",
    "선물", "WTI", "유가", "원유"
]
# ─────────────────────────────────────────────────────


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def get_recent_business_day(days_back=1):
    date = datetime.today() - timedelta(days=days_back)
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    return date.strftime("%Y%m%d")


def get_dates():
    base = get_recent_business_day(1)
    base_dt = datetime.strptime(base, "%Y%m%d")
    start_dt = base_dt - timedelta(days=LOOKBACK_DAYS)
    return start_dt.strftime("%Y%m%d"), base, base


def is_equity_etf(name: str) -> bool:
    for kw in EXCLUDE_KEYWORDS:
        if kw in name:
            return False
    return True


def to_df(result) -> pd.DataFrame:
    if result is None:
        return pd.DataFrame()
    if isinstance(result, pd.DataFrame):
        return result
    if isinstance(result, dict):
        for key in result:
            val = result[key]
            if isinstance(val, list) and len(val) > 0:
                return pd.DataFrame(val)
        return pd.DataFrame()
    if isinstance(result, list):
        return pd.DataFrame(result)
    return pd.DataFrame()


def find_col(df, keywords):
    for kw in keywords:
        if kw in df.columns:
            return kw
    for c in df.columns:
        for kw in keywords:
            if kw in str(c):
                return c
    return None


def get_etf_data(client: KRXOpenAPI, base_date: str) -> dict:
    """ETF 유니버스 + 시총을 한번에 수집"""
    log("ETF 유니버스 수집 중...")
    try:
        result = client.get_etf_daily_trade(bas_dd=base_date)
        df = to_df(result)
        if df.empty:
            log("  → ETF 데이터 없음")
            return {}
        log(f"  → ETF 컬럼: {df.columns.tolist()}")
    except Exception as e:
        log(f"  → ETF 조회 오류: {type(e).__name__}: {e}")
        return {}

    code_col = find_col(df, ["ISU_CD", "ISU_SRT_CD", "종목코드", "단축코드"])
    name_col = find_col(df, ["ISU_NM", "종목명"])
    cap_col  = find_col(df, ["MKTCAP", "시가총액"])

    if not code_col or not name_col:
        log(f"  → 컬럼 매핑 실패: {df.columns.tolist()}")
        return {}

    etf_info = {}
    for _, row in df.iterrows():
        try:
            ticker = str(row[code_col]).strip().zfill(6)
            name   = str(row[name_col]).strip()
            mktcap = float(str(row[cap_col]).replace(",", "") or 0) if cap_col else 0
            if len(ticker) == 6 and ticker.isdigit() and is_equity_etf(name):
                etf_info[ticker] = {"name": name, "mktcap": mktcap}
        except:
            continue

    log(f"  → 국내 주식형 ETF {len(etf_info)}개 선별")
    return etf_info


def get_etf_buys(client: KRXOpenAPI, start: str, end: str, etf_tickers: set) -> dict:
    """기간 내 ETF별 거래대금 집계"""
    etf_buys = {}
    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt   = datetime.strptime(end, "%Y%m%d")
    current  = start_dt
    day_count = 0

    while current <= end_dt:
        if current.weekday() < 5:
            date_str = current.strftime("%Y%m%d")
            day_count += 1
            try:
                result = client.get_etf_daily_trade(bas_dd=date_str)
                df = to_df(result)
                if not df.empty:
                    code_col = find_col(df, ["ISU_CD", "ISU_SRT_CD"])
                    val_col  = find_col(df, ["ACC_TRDVAL", "거래대금"])
                    if code_col and val_col:
                        for _, row in df.iterrows():
                            try:
                                ticker = str(row[code_col]).strip().zfill(6)
                                if ticker not in etf_tickers:
                                    continue
                                raw = str(row[val_col]).replace(",", "").replace("-", "0")
                                val = float(raw or 0)
                                if val > 0:
                                    etf_buys[ticker] = etf_buys.get(ticker, 0) + val
                            except:
                                continue
                log(f"  {date_str} 처리완료 ({day_count}일차)")
                time.sleep(0.2)
            except Exception as e:
                log(f"  {date_str} 오류: {e}")
        current += timedelta(days=1)

    return etf_buys


def fmt(n: float) -> str:
    if n >= 1e12: return f"{n/1e12:.1f}조"
    if n >= 1e8:  return f"{n/1e8:.0f}억"
    return f"{n:,.0f}"


def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN:
        print(text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "parse_mode": "HTML"
            }, timeout=10)
            time.sleep(0.5)
        except Exception as e:
            log(f"텔레그램 오류: {e}")


def main():
    log("=" * 50)
    log("ETF 수급 스크리너 시작")
    log("=" * 50)

    if not KRX_API_KEY:
        log("KRX_API_KEY 없음. 종료.")
        return

    log(f"KRX API Key: {KRX_API_KEY[:8]}...")
    client = KRXOpenAPI(api_key=KRX_API_KEY, rate_limit=5, per_seconds=1)

    start_date, end_date, base_date = get_dates()
    log(f"분석기간: {start_date} ~ {end_date} ({LOOKBACK_DAYS}일)")

    # STEP 1: ETF 유니버스 + 시총 수집
    etf_info = get_etf_data(client, base_date)
    if not etf_info:
        log("ETF 리스트 없음. 종료.")
        send_telegram("❌ ETF 스크리너: ETF 리스트 조회 실패")
        return

    log(f"유니버스: {len(etf_info)}개 ETF")

    # STEP 2: 기간 거래대금 집계
    log(f"\nETF 거래대금 집계 중...")
    etf_buys = get_etf_buys(client, start_date, end_date, set(etf_info.keys()))
    log(f"  → 집계: {len(etf_buys)}개")

    # STEP 3: 필터링
    filtered = {k: v for k, v in etf_buys.items() if v >= MIN_ETF_INFLOW}
    log(f"  → 10억 이상: {len(filtered)}개")

    if not filtered:
        log("필터 통과 없음. 상위 20개로 완화.")
        filtered = dict(sorted(etf_buys.items(), key=lambda x: x[1], reverse=True)[:20])

    # STEP 4: 결과 조합
    results = []
    for ticker, inflow in filtered.items():
        info   = etf_info.get(ticker, {})
        name   = info.get("name", ticker)
        mktcap = info.get("mktcap", 0)
        conds  = "🎯ETF수급"
        if 0 < mktcap < 1_000_000_000_000:
            conds += " 🔹소형"
        results.append({"ticker": ticker, "name": name, "inflow": inflow,
                        "mktcap": mktcap, "score": inflow, "conds": conds})

    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:TOP_N]
    log(f"\n  → 최종 선별: {len(top)}개")

    # STEP 5: 텔레그램 발송
    now = datetime.now().strftime("%Y/%m/%d %H:%M")
    msg = f"📊 <b>ETF 수급 스크리너</b> | {now}\n"
    msg += f"분석기간: {start_date[4:6]}/{start_date[6:]} ~ {end_date[4:6]}/{end_date[6:]} (1주)\n"
    msg += f"선별: <b>{len(top)}개 ETF</b>\n"
    msg += "─" * 28 + "\n\n"

    for i, r in enumerate(top, 1):
        cap_str = fmt(r["mktcap"]) if r["mktcap"] > 0 else "N/A"
        msg += (f"<b>{i}. {r['name']} ({r['ticker']})</b>\n"
                f"  거래대금: {fmt(r['inflow'])} | 순자산: {cap_str}\n"
                f"  {r['conds']}\n\n")

    send_telegram(msg)
    log("\n완료!")


if __name__ == "__main__":
    main()
