"""
ETF 수급 기반 종목 스크리너 v3
미래에셋증권 "신(新) 수급의 시대" 전략 구현

흐름:
  1. KRX API  → ETF 유니버스 + 기간 거래대금 집계
  2. KIS API  → 상위 ETF별 구성종목(PDF) + 편입비중 조회
  3. 거래대금 × 편입비중 → 종목별 ETF 유입 기여액 합산
  4. 유가증권/코스닥 종목 정보 결합 → 상위 20개 텔레그램 발송
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from pykrx_openapi import KRXOpenAPI

# ─── 설정 ────────────────────────────────────────────
KRX_API_KEY        = os.environ.get("KRX_API_KEY", "")
KIS_APP_KEY        = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET     = os.environ.get("KIS_APP_SECRET", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "@saladentnews")

KIS_BASE_URL       = "https://openapi.koreainvestment.com:9443"

MIN_STOCK_INFLOW = 3_000_000_000   # 종목별 최소 ETF 유입 기여액 30억
TOP_ETF_N        = 30              # 편입종목 역추적할 상위 ETF 수
TOP_N            = 20              # 최종 종목 선별 수
LOOKBACK_DAYS    = 7               # 분석 기간 (달력 기준)

# ETF 제외 키워드
EXCLUDE_KEYWORDS = [
    "레버리지", "인버스", "2X", "3X", "-1X", "곱버스",
    "해외", "미국", "중국", "일본", "인도", "베트남", "나스닥", "S&P",
    "채권", "국채", "달러", "금", "선물", "WTI", "유가", "원유",
    "커버드콜", "covered", "프리미엄", "위클리",
    "머니마켓", "MMF", "단기", "CD금리", "KOFR", "SOFR",
    "부동산", "리츠", "REIT",
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


def is_valid_etf(name: str) -> bool:
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


# ─── KIS API ─────────────────────────────────────────

def get_kis_token() -> str:
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
    }
    try:
        r = requests.post(url, json=body, timeout=10)
        return r.json().get("access_token", "")
    except Exception as e:
        log(f"  KIS 토큰 오류: {e}")
        return ""


def get_etf_components_kis(etf_ticker: str, token: str) -> list:
    """KIS - ETF 구성종목시세 (TR: FHKST121600C0)"""
    url = f"{KIS_BASE_URL}/uapi/etfetn/v1/quotations/inquire-component-stock-price"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST121600C0",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": etf_ticker,
        "FID_COND_SCR_DIV_CODE": "11216",
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        data = r.json()
        output2 = data.get("output2", [])
        holdings = []
        for row in output2:
            code   = str(row.get("stck_shrn_iscd", "")).strip().zfill(6)
            name   = str(row.get("hts_kor_isnm", "")).strip()
            weight = float(str(row.get("etf_cnfg_issu_rt", 0) or
                               row.get("btp_issu_rt", 0) or 0))
            if len(code) == 6 and code.isdigit() and weight > 0:
                holdings.append({"ticker": code, "name": name, "weight": weight})
        return holdings
    except Exception as e:
        log(f"    KIS PDF 오류 ({etf_ticker}): {e}")
        return []


# ─── KRX API ─────────────────────────────────────────

def get_etf_universe(client: KRXOpenAPI, base_date: str) -> dict:
    log("ETF 유니버스 수집 중...")
    try:
        df = to_df(client.get_etf_daily_trade(bas_dd=base_date))
        if df.empty:
            log("  → ETF 데이터 없음")
            return {}
    except Exception as e:
        log(f"  → ETF 조회 오류: {e}")
        return {}

    code_col = find_col(df, ["ISU_CD", "ISU_SRT_CD"])
    name_col = find_col(df, ["ISU_NM"])
    cap_col  = find_col(df, ["MKTCAP"])

    if not code_col or not name_col:
        log(f"  → 컬럼 매핑 실패: {df.columns.tolist()}")
        return {}

    etf_info = {}
    filtered_out = 0
    for _, row in df.iterrows():
        try:
            ticker = str(row[code_col]).strip().zfill(6)
            name   = str(row[name_col]).strip()
            mktcap = float(str(row[cap_col]).replace(",", "") or 0) if cap_col else 0
            if len(ticker) == 6 and ticker.isdigit():
                if is_valid_etf(name):
                    etf_info[ticker] = {"name": name, "mktcap": mktcap}
                else:
                    filtered_out += 1
        except:
            continue

    log(f"  → 유효 ETF {len(etf_info)}개 | 제외 {filtered_out}개")
    return etf_info


def get_etf_buys(client: KRXOpenAPI, start: str, end: str, etf_tickers: set) -> dict:
    etf_buys = {}
    current = datetime.strptime(start, "%Y%m%d")
    end_dt  = datetime.strptime(end, "%Y%m%d")
    day_n   = 0

    while current <= end_dt:
        if current.weekday() < 5:
            date_str = current.strftime("%Y%m%d")
            day_n += 1
            try:
                df = to_df(client.get_etf_daily_trade(bas_dd=date_str))
                if not df.empty:
                    code_col = find_col(df, ["ISU_CD", "ISU_SRT_CD"])
                    val_col  = find_col(df, ["ACC_TRDVAL"])
                    if code_col and val_col:
                        for _, row in df.iterrows():
                            try:
                                t = str(row[code_col]).strip().zfill(6)
                                if t not in etf_tickers:
                                    continue
                                v = float(str(row[val_col]).replace(",", "").replace("-", "0") or 0)
                                if v > 0:
                                    etf_buys[t] = etf_buys.get(t, 0) + v
                            except:
                                continue
                log(f"  {date_str} 완료 ({day_n}일차)")
                time.sleep(0.2)
            except Exception as e:
                log(f"  {date_str} 오류: {e}")
        current += timedelta(days=1)

    return etf_buys


def get_stock_info_bulk(client: KRXOpenAPI, base_date: str) -> dict:
    stock_info = {}
    for get_fn in [client.get_stock_daily_trade, client.get_kosdaq_stock_daily_trade]:
        try:
            df = to_df(get_fn(bas_dd=base_date))
            if df.empty:
                continue
            code_col = find_col(df, ["ISU_CD", "ISU_SRT_CD"])
            name_col = find_col(df, ["ISU_NM"])
            cap_col  = find_col(df, ["MKTCAP"])
            if not code_col or not name_col:
                continue
            for _, row in df.iterrows():
                try:
                    t = str(row[code_col]).strip().zfill(6)
                    n = str(row[name_col]).strip()
                    c = float(str(row[cap_col]).replace(",", "") or 0) if cap_col else 0
                    stock_info[t] = {"name": n, "mktcap": c}
                except:
                    continue
            time.sleep(0.3)
        except:
            continue
    log(f"  → 주식 종목 {len(stock_info)}개 수집")
    return stock_info


# ─── 유틸 ────────────────────────────────────────────

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


def _fallback(etf_info, etf_buys, start_date, end_date):
    top_etfs = sorted(
        [(k, v) for k, v in etf_buys.items() if k in etf_info],
        key=lambda x: x[1], reverse=True
    )[:20]
    now = datetime.now().strftime("%Y/%m/%d %H:%M")
    msg = f"📊 <b>ETF 수급 스크리너 (ETF 결과)</b> | {now}\n"
    msg += f"분석기간: {start_date[4:6]}/{start_date[6:]} ~ {end_date[4:6]}/{end_date[6:]}\n"
    msg += "⚠️ 편입종목 조회 실패 — ETF 거래대금 기준\n"
    msg += "─" * 28 + "\n\n"
    for i, (t, v) in enumerate(top_etfs, 1):
        info = etf_info.get(t, {})
        cap_str = fmt(info.get("mktcap", 0)) if info.get("mktcap", 0) > 0 else "N/A"
        msg += (f"<b>{i}. {info.get('name', t)} ({t})</b>\n"
                f"  거래대금: {fmt(v)} | 순자산: {cap_str}\n\n")
    send_telegram(msg)


# ─── 메인 ────────────────────────────────────────────

def main():
    log("=" * 50)
    log("ETF 수급 스크리너 v3 시작")
    log("=" * 50)

    if not KRX_API_KEY:
        log("KRX_API_KEY 없음. 종료.")
        return
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        log("KIS_APP_KEY/SECRET 없음. 종료.")
        return

    krx = KRXOpenAPI(api_key=KRX_API_KEY, rate_limit=5, per_seconds=1)
    start_date, end_date, base_date = get_dates()
    log(f"분석기간: {start_date} ~ {end_date}")

    # STEP 1: ETF 유니버스
    etf_info = get_etf_universe(krx, base_date)
    if not etf_info:
        send_telegram("❌ ETF 스크리너: ETF 조회 실패")
        return
    log(f"유니버스: {len(etf_info)}개 ETF")

    # STEP 2: 기간 거래대금
    log("\nETF 거래대금 집계 중...")
    etf_buys = get_etf_buys(krx, start_date, end_date, set(etf_info.keys()))
    log(f"  → {len(etf_buys)}개 집계")

    top_etfs = sorted(etf_buys.items(), key=lambda x: x[1], reverse=True)[:TOP_ETF_N]
    log(f"  → 상위 {len(top_etfs)}개 ETF 편입종목 역추적")

    # STEP 3: KIS 토큰
    log("\nKIS 토큰 발급 중...")
    kis_token = get_kis_token()
    if not kis_token:
        log("  → KIS 토큰 실패. fallback.")
        _fallback(etf_info, etf_buys, start_date, end_date)
        return
    log("  → 토큰 발급 성공")

    # STEP 4: 편입종목 역추적
    log("\n편입종목 역추적 중...")
    stock_inflow = {}
    pdf_ok = 0

    for etf_ticker, etf_vol in top_etfs:
        holdings = get_etf_components_kis(etf_ticker, kis_token)
        if not holdings:
            time.sleep(0.2)
            continue
        pdf_ok += 1
        total_wt = sum(h["weight"] for h in holdings)
        for h in holdings:
            stk    = h["ticker"]
            wt     = h["weight"] / total_wt if total_wt > 0 else 0
            contrib = etf_vol * wt
            stock_inflow[stk] = stock_inflow.get(stk, 0) + contrib
        time.sleep(0.15)

    log(f"  → PDF 성공: {pdf_ok}/{len(top_etfs)} | 집계 종목: {len(stock_inflow)}개")

    if pdf_ok == 0:
        _fallback(etf_info, etf_buys, start_date, end_date)
        return

    # STEP 5: 종목 정보
    log("\n종목 정보 수집 중...")
    stock_info = get_stock_info_bulk(krx, base_date)

    # STEP 6: 필터 + 정렬
    results = []
    for ticker, inflow in stock_inflow.items():
        if inflow < MIN_STOCK_INFLOW:
            continue
        info   = stock_info.get(ticker, {})
        name   = info.get("name", "")
        mktcap = info.get("mktcap", 0)
        if not name:
            continue
        conds = "🎯ETF수급"
        if 0 < mktcap < 1_000_000_000_000:
            conds += " 🔹소형주"
        results.append({"ticker": ticker, "name": name, "inflow": inflow,
                        "mktcap": mktcap, "score": inflow, "conds": conds})

    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:TOP_N]
    log(f"\n  → 조건 충족: {len(results)}개 | 최종 선별: {len(top)}개")

    # STEP 7: 발송
    now = datetime.now().strftime("%Y/%m/%d %H:%M")
    msg = f"📊 <b>ETF 수급 종목 스크리너</b> | {now}\n"
    msg += f"분석기간: {start_date[4:6]}/{start_date[6:]} ~ {end_date[4:6]}/{end_date[6:]}\n"
    msg += f"ETF {len(top_etfs)}개 역추적 → <b>{len(top)}개 종목</b> 선별\n"
    msg += "─" * 28 + "\n\n"

    if not top:
        msg += "조건 충족 종목이 없습니다.\n"
    else:
        for i, r in enumerate(top, 1):
            cap_str = fmt(r["mktcap"]) if r["mktcap"] > 0 else "N/A"
            msg += (f"<b>{i}. {r['name']} ({r['ticker']})</b>\n"
                    f"  ETF유입기여: {fmt(r['inflow'])} | 시총: {cap_str}\n"
                    f"  {r['conds']}\n\n")

    send_telegram(msg)
    log("\n완료!")


if __name__ == "__main__":
    main()
