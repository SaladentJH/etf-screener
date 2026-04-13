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

MIN_MARKET_CAP  = 200_000_000_000
MIN_ETF_INFLOW  = 10_000_000_000
TOP_N           = 20

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
    start_dt = base_dt - timedelta(days=20)
    return start_dt.strftime("%Y%m%d"), base, base


def is_equity_etf(name: str) -> bool:
    for kw in EXCLUDE_KEYWORDS:
        if kw in name:
            return False
    return True


def to_df(result) -> pd.DataFrame:
    """pykrx_openapi 반환값을 DataFrame으로 변환"""
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
    for c in df.columns:
        for kw in keywords:
            if kw in str(c):
                return c
    return None


def get_etf_universe(client: KRXOpenAPI, base_date: str) -> list:
    log("ETF 유니버스 수집 중...")
    try:
        result = client.get_etf_daily_trade(bas_dd=base_date)
        df = to_df(result)
        if df.empty:
            log("  → ETF 데이터 없음")
            return []
        log(f"  → ETF 컬럼: {df.columns.tolist()}")
        log(f"  → ETF 샘플:\n{df.head(2).to_string()}")
    except Exception as e:
        log(f"  → ETF 조회 오류: {type(e).__name__}: {e}")
        return []

    code_col = find_col(df, ["종목코드", "단축코드", "ISU_SRT_CD", "isuSrtCd"])
    name_col = find_col(df, ["종목명", "ISU_NM", "isuNm"])

    if not code_col or not name_col:
        log(f"  → 컬럼 매핑 실패. 전체컬럼: {df.columns.tolist()}")
        return []

    universe = []
    for _, row in df.iterrows():
        try:
            ticker = str(row[code_col]).strip().zfill(6)
            name = str(row[name_col]).strip()
            if len(ticker) == 6 and ticker.isdigit() and is_equity_etf(name):
                universe.append({"ticker": ticker, "name": name})
        except:
            continue

    log(f"  → 국내 주식형 ETF {len(universe)}개 선별")
    return universe


def get_etf_net_buy_period(client: KRXOpenAPI, start: str, end: str) -> dict:
    etf_buys = {}
    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    current = start_dt

    while current <= end_dt:
        if current.weekday() < 5:
            date_str = current.strftime("%Y%m%d")
            try:
                result = client.get_etf_daily_trade(bas_dd=date_str)
                df = to_df(result)
                if not df.empty:
                    code_col = find_col(df, ["종목코드", "단축코드", "ISU_SRT_CD", "isuSrtCd"])
                    buy_col  = find_col(df, ["기관순매수", "기관_순매수", "기관 순매수", "InstNetBuyVol", "instNetBuyVol"])
                    val_col  = find_col(df, ["거래대금", "TDD_TRDVAL", "tddTrdval"])

                    if code_col:
                        use_col = buy_col or val_col
                        for _, row in df.iterrows():
                            try:
                                ticker = str(row[code_col]).strip().zfill(6)
                                raw = str(row[use_col]).replace(",", "").replace("-", "0") if use_col else "0"
                                val = float(raw or 0)
                                if val > 0:
                                    etf_buys[ticker] = etf_buys.get(ticker, 0) + val
                            except:
                                continue
                time.sleep(0.2)
            except Exception as e:
                log(f"  날짜 {date_str} 오류: {e}")
        current += timedelta(days=1)

    return etf_buys


def get_stock_info(client: KRXOpenAPI, ticker: str, end_date: str) -> dict:
    for get_fn in [client.get_stock_daily_trade, client.get_kosdaq_stock_daily_trade]:
        try:
            result = get_fn(bas_dd=end_date)
            df = to_df(result)
            if df.empty:
                continue
            code_col = find_col(df, ["종목코드", "단축코드", "ISU_SRT_CD", "isuSrtCd"])
            if not code_col:
                continue
            row = df[df[code_col].astype(str).str.zfill(6) == ticker]
            if row.empty:
                continue

            cap_col   = find_col(df, ["시가총액", "MKTCAP", "mktcap"])
            price_col = find_col(df, ["종가", "현재가", "TDD_CLSPRC", "tddClsprc"])
            name_col  = find_col(df, ["종목명", "ISU_NM", "isuNm"])

            market_cap = float(str(row[cap_col].iloc[0]).replace(",", "") or 0) if cap_col else 0
            price      = float(str(row[price_col].iloc[0]).replace(",", "") or 0) if price_col else 0
            name       = str(row[name_col].iloc[0]).strip() if name_col else ticker

            if market_cap >= MIN_MARKET_CAP and price > 0:
                return {"ticker": ticker, "name": name, "market_cap": market_cap, "price": price}
        except:
            continue
    return {}


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
    client = KRXOpenAPI(api_key=KRX_API_KEY, rate_limit=5, per_seconds=1, debug=True)

    start_date, end_date, base_date = get_dates()
    log(f"분석기간: {start_date} ~ {end_date}")

    # STEP 1: ETF 유니버스
    etf_list = get_etf_universe(client, base_date)
    if not etf_list:
        log("ETF 리스트 없음. 종료.")
        send_telegram("❌ ETF 스크리너: ETF 리스트 조회 실패")
        return

    etf_tickers = {e["ticker"]: e["name"] for e in etf_list}
    log(f"유니버스: {len(etf_tickers)}개 ETF")

    # STEP 2: 기간 순매수 집계
    log(f"\nETF 기간 순매수 집계 중... ({start_date}~{end_date})")
    etf_buys = get_etf_net_buy_period(client, start_date, end_date)
    log(f"  → 집계 ETF 수: {len(etf_buys)}개")

    filtered = {k: v for k, v in etf_buys.items() if k in etf_tickers and v >= MIN_ETF_INFLOW}
    log(f"  → 10억 이상 유입: {len(filtered)}개")

    if not filtered:
        log("필터 통과 없음. 상위 20개로 완화.")
        filtered = {k: v for k, v in sorted(etf_buys.items(), key=lambda x: x[1], reverse=True)[:20] if k in etf_tickers}

    # STEP 3: 종목 정보 + 결과
    log(f"\n종목 정보 수집 중...")
    results = []

    for ticker, inflow in filtered.items():
        info       = get_stock_info(client, ticker, end_date)
        name       = info.get("name") or etf_tickers.get(ticker, ticker)
        market_cap = info.get("market_cap", 0)
        conds      = "🎯ETF수급"
        if 0 < market_cap < 1_000_000_000_000:
            conds += " 🔹소형"

        results.append({
            "ticker": ticker,
            "name": name,
            "inflow": inflow,
            "market_cap": market_cap,
            "score": inflow,
            "conds": conds,
        })
        time.sleep(0.05)

    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:TOP_N]
    log(f"\n  → 최종 선별: {len(top)}개")

    # STEP 4: 텔레그램 발송
    now = datetime.now().strftime("%Y/%m/%d %H:%M")
    msg = f"📊 <b>ETF 수급 스크리너</b> | {now}\n"
    msg += f"분석기간: {start_date[4:6]}/{start_date[6:]} ~ {end_date[4:6]}/{end_date[6:]}\n"
    msg += f"선별: <b>{len(top)}개 ETF</b>\n"
    msg += "─" * 28 + "\n\n"

    if not top:
        msg += "조건 충족 종목이 없습니다.\n"
    else:
        for i, r in enumerate(top, 1):
            cap_str = fmt(r["market_cap"]) if r["market_cap"] > 0 else "N/A"
            msg += (
                f"<b>{i}. {r['name']} ({r['ticker']})</b>\n"
                f"  ETF유입: {fmt(r['inflow'])} | 시총: {cap_str}\n"
                f"  {r['conds']}\n\n"
            )

    send_telegram(msg)
    log("\n완료!")


if __name__ == "__main__":
    main()
