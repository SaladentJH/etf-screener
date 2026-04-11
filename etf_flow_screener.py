"""
ETF 수급 기반 종목 스크리너
미래에셋증권 "신(新) 수급의 시대" 전략 구현

실행: GitHub Actions (매주 월요일 08:00 KST 자동 실행)
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock as pykrx

# ─── 설정 ────────────────────────────────────────────
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


def get_etf_universe() -> list:
    log("ETF 유니버스 수집 중...")
    tickers = []
    used_date = None

    # 최근 10 거래일 순서로 시도
    for days_back in range(1, 15):
        date = get_recent_business_day(days_back)
        try:
            log(f"  날짜 {date} 시도...")
            try:
                result = pykrx.get_etf_ticker_list(date)
            except TypeError:
                result = pykrx.get_etf_ticker_list()
            if result and len(result) > 0:
                tickers = result
                used_date = date
                log(f"  → 성공: {date} 기준 {len(tickers)}개 ETF")
                break
            else:
                log(f"  → {date} 결과 없음")
        except KeyError as e:
            log(f"  → {date} KeyError({e}) - 다음 날짜 시도")
            time.sleep(0.3)
            continue
        except Exception as e:
            log(f"  → {date} 오류({type(e).__name__}: {e}) - 다음 날짜 시도")
            time.sleep(0.3)
            continue

    # 그래도 실패하면 날짜 없이 한번 더 시도
    if not tickers:
        try:
            log("  날짜 인자 없이 최종 시도...")
            tickers = pykrx.get_etf_ticker_list()
            used_date = get_recent_business_day(1)
            log(f"  → 성공: {len(tickers)}개")
        except Exception as e:
            log(f"  → 최종 시도 실패: {e}")
            return []

    universe = []
    for t in tickers:
        try:
            name = pykrx.get_etf_ticker_name(t)
            if is_equity_etf(name):
                universe.append({"ticker": t, "name": name, "base_date": used_date})
            time.sleep(0.03)
        except:
            continue

    log(f"  → 국내 주식형 ETF {len(universe)}개 선별")
    return universe


def get_etf_net_buy(ticker: str, start: str, end: str) -> float:
    try:
        df = pykrx.get_market_trading_value_by_date(start, end, ticker)
        if df is None or df.empty:
            return 0.0
        cols = df.columns.tolist()
        for col in ["기관합계", "기관", "금융투자", "투신"]:
            if col in cols:
                return float(df[col].sum())
        if len(cols) >= 2:
            return float(df.iloc[:, 1].sum())
        return 0.0
    except:
        return 0.0


def get_etf_holdings(ticker: str, date: str) -> dict:
    try:
        df = pykrx.get_etf_portfolio_deposit_file(ticker, date)
        if df is None or df.empty:
            return {}
        result = {}
        for _, row in df.iterrows():
            try:
                row_dict = row.to_dict()
                code = ""
                for key in ["티커", "종목코드", "Ticker", "ticker"]:
                    if key in row_dict:
                        code = str(row_dict[key]).strip().zfill(6)
                        break
                weight = 0.0
                for key in ["비중", "구성비중", "편입비중", "Weight", "weight"]:
                    if key in row_dict:
                        weight = float(row_dict[key])
                        break
                if len(code) == 6 and code.isdigit() and weight > 0:
                    result[code] = weight
            except:
                continue
        return result
    except:
        return {}


def get_stock_metrics(ticker: str, start: str, end: str) -> dict:
    try:
        cap_df = pykrx.get_market_cap_by_date(start, end, ticker)
        if cap_df is None or cap_df.empty:
            return {}
        market_cap = float(cap_df["시가총액"].iloc[-1])
        if market_cap < MIN_MARKET_CAP:
            return {}

        ohlcv = pykrx.get_market_ohlcv_by_date(start, end, ticker)
        if ohlcv is None or ohlcv.empty or len(ohlcv) < 5:
            return {}

        current = float(ohlcv["종가"].iloc[-1])
        ma5 = float(ohlcv["종가"].tail(5).mean())
        disparity = (current / ma5 - 1) * 100
        name = pykrx.get_market_ticker_name(ticker)

        return {
            "ticker": ticker,
            "name": name,
            "market_cap": market_cap,
            "price": current,
            "disparity": disparity,
        }
    except:
        return {}


def calc_score(inflow, market_cap, disparity, max_weight, is_focused):
    base = inflow / 1_000_000_000
    cond1 = disparity < -2.0
    cond2 = market_cap < 1_000_000_000_000
    cond3 = is_focused
    if cond1: base *= 1.5
    if cond2: base *= 1.3
    if cond3: base *= 1.4
    if max_weight >= 5.0: base *= 1.2
    return base, cond1, cond2, cond3


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

    try:
        import pykrx as pk_module
        log(f"pykrx 버전: {pk_module.__version__}")
    except:
        pass

    start_date, end_date, base_date = get_dates()
    log(f"분석기간: {start_date} ~ {end_date}")

    # STEP 1: ETF 유니버스
    etf_list = get_etf_universe()
    if not etf_list:
        log("ETF 리스트 없음. 종료.")
        send_telegram("❌ ETF 스크리너 오류: ETF 리스트 조회 실패")
        return

    base_date = etf_list[0]["base_date"]

    # PDF 컬럼 구조 확인 (디버깅)
    log("첫 ETF PDF 컬럼 확인...")
    try:
        sample_df = pykrx.get_etf_portfolio_deposit_file(etf_list[0]["ticker"], base_date)
        if sample_df is not None and not sample_df.empty:
            log(f"  PDF 컬럼: {sample_df.columns.tolist()}")
            log(f"  PDF 샘플:\n{sample_df.head(2).to_string()}")
    except Exception as e:
        log(f"  PDF 샘플 오류: {e}")

    # STEP 2+3: ETF별 순매수 + 편입종목 집계
    log(f"\nETF 자금 유입 및 편입종목 집계 중... ({len(etf_list)}개)")
    stock_inflow   = {}
    stock_weight   = {}
    focused_stocks = set()

    for i, etf in enumerate(etf_list):
        if i % 30 == 0:
            log(f"  진행: {i}/{len(etf_list)}")

        ticker = etf["ticker"]
        net_buy = get_etf_net_buy(ticker, start_date, end_date)
        if net_buy <= 0:
            time.sleep(0.1)
            continue

        holdings = get_etf_holdings(ticker, base_date)
        if not holdings:
            time.sleep(0.1)
            continue

        is_focused = len(holdings) <= 20

        for stk, wt in holdings.items():
            contribution = net_buy * (wt / 100)
            stock_inflow[stk] = stock_inflow.get(stk, 0) + contribution
            stock_weight[stk] = max(stock_weight.get(stk, 0), wt)
            if is_focused:
                focused_stocks.add(stk)

        time.sleep(0.1)

    log(f"\n  → 집계 종목 수: {len(stock_inflow)}개")

    filtered = {k: v for k, v in stock_inflow.items() if v >= MIN_ETF_INFLOW}
    log(f"  → 유입 10억 이상: {len(filtered)}개")

    if not filtered:
        log("필터 통과 종목 없음. 상위 50개로 완화.")
        filtered = dict(sorted(stock_inflow.items(), key=lambda x: x[1], reverse=True)[:50])

    # STEP 4: 종목 정보 수집 + 스코어링
    log(f"\n종목 정보 수집 및 스코어링 중...")
    results = []

    for stk, inflow in filtered.items():
        info = get_stock_metrics(stk, start_date, end_date)
        if not info:
            time.sleep(0.05)
            continue

        max_wt = stock_weight.get(stk, 0)
        is_focused = stk in focused_stocks

        score, c1, c2, c3 = calc_score(
            inflow, info["market_cap"], info["disparity"], max_wt, is_focused
        )

        cond_count = sum([c1, c2, c3])
        if cond_count == 0:
            continue

        conds = []
        if c1: conds.append("📉단기하락")
        if c2: conds.append("🔹소형주")
        if c3: conds.append("🎯집중ETF")

        results.append({
            **info,
            "inflow": inflow,
            "max_weight": max_wt,
            "score": score,
            "conds": " ".join(conds),
            "cond_count": cond_count,
        })
        time.sleep(0.05)

    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:TOP_N]
    log(f"\n  → 조건 충족: {len(results)}개 | 최종 선별: {len(top)}개")

    # STEP 5: 텔레그램 발송
    now = datetime.now().strftime("%Y/%m/%d %H:%M")
    msg = f"📊 <b>ETF 수급 스크리너</b> | {now}\n"
    msg += f"분석기간: {start_date[4:6]}/{start_date[6:]} ~ {end_date[4:6]}/{end_date[6:]}\n"
    msg += f"선별: <b>{len(top)}종목</b> (전체 충족 {len(results)}개)\n"
    msg += "─" * 28 + "\n\n"

    if not top:
        msg += "조건 충족 종목이 없습니다.\n"
    else:
        for i, r in enumerate(top, 1):
            msg += (
                f"<b>{i}. {r['name']} ({r['ticker']})</b>\n"
                f"  ETF유입: {fmt(r['inflow'])} | 이격도: {r['disparity']:+.1f}%\n"
                f"  시총: {fmt(r['market_cap'])} | ETF편입비중: {r['max_weight']:.1f}%\n"
                f"  {r['conds']}\n\n"
            )

    send_telegram(msg)
    log("\n완료!")


if __name__ == "__main__":
    main()
