"""
ETF 수급 기반 종목 스크리너 v5.13
미래에셋증권 "신(新) 수급의 시대" 전략 구현

전략 스킴:
  개인 자금 → ETF 유입 → PDF 역추적 → 개별종목 수급 파악
  외국인 동반 수급 여부 확인 → 수급강도 측정

수급강도 지표 (모두 % 단위):
  ETF수급강도(%)  = ETF유입기여 / 거래대금 5일 일평균 × 100
                   → 미래에셋 원래 스킴, ETF 수급 압력 측정
  개인수급강도(%) = 개인순매수 5일 일평균 / 거래대금 5일 일평균 × 100
                   → 개인이 직접 사는 강도 측정

변경 (v5.12 → v5.13):
  - 두 수급강도 모두 % 단위로 통일 (소수점 1자리)
  - ETF수급강도 복원 (v5.12에서 제거됐던 것)
  - 정렬 기준: ETF수급강도 내림차순 (미래에셋 원래 스킴 기준)
"""

import os
import json
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

KIS_BASE_URL   = "https://openapi.koreainvestment.com:9443"
AUM_CACHE_FILE = "etf_aum_cache.json"

# ─── 분석 파라미터 ─────────────────────────────────────
MIN_STOCK_INFLOW    = 3_000_000_000
MIN_LIQUIDITY_20D   = 20_000_000_000
MIN_MKTCAP          = 1_000_000_000_000
TOP_ETF_N           = 30
CANDIDATE_N         = 30
TOP_N               = 30
LOOKBACK_DAYS       = 7
DISPARITY_PERIOD    = 20
DISPARITY_THRESH    = -2.0
MAX_ETF_STOCKS      = 30
PRSN_INTENSITY_DAYS = 5   # 개인수급강도 계산 고정 기간

INVESTOR_DAYS_BY_WEEKDAY = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5}

EXCLUDE_KEYWORDS = [
    "레버리지", "인버스", "2X", "3X", "-1X", "곱버스",
    "해외", "미국", "중국", "일본", "인도", "베트남", "나스닥", "S&P",
    "유럽", "신흥국", "이머징", "글로벌", "아시아", "홍콩", "대만",
    "브라질", "러시아", "인도네시아", "멕시코",
    "채권", "국채", "국고채", "회사채", "하이일드", "크레딧",
    "금리", "듀레이션", "통안채", "은행채", "특수채", "물가채",
    "10년", "30년", "3년", "5년", "단기채", "중기채", "장기채",
    "TLT", "AGG", "IEF", "LQD", "HYG",
    "우량채", "혼합채", "채권혼합",
    "달러", "엔화", "위안", "유로", "환헤지", "환노출",
    "금", "은", "구리", "선물", "WTI", "유가", "원유", "천연가스",
    "농산물", "원자재", "상품",
    "커버드콜", "covered", "프리미엄", "위클리", "옵션",
    "버퍼", "테일헤지",
    "머니마켓", "MMF", "단기", "CD금리", "KOFR", "SOFR",
    "CP", "콜", "RP",
    "부동산", "리츠", "REIT", "인프라",
    "혼합", "멀티에셋", "인컴", "TDF",
]


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def is_valid_etf(name: str) -> bool:
    return all(kw not in name for kw in EXCLUDE_KEYWORDS)


def get_recent_business_day(days_back: int = 1) -> str:
    date = datetime.today() - timedelta(days=days_back)
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    return date.strftime("%Y%m%d")


def get_investor_days() -> int:
    return INVESTOR_DAYS_BY_WEEKDAY.get(datetime.today().weekday(), 5)


def to_df(result) -> pd.DataFrame:
    if result is None:
        return pd.DataFrame()
    if isinstance(result, pd.DataFrame):
        return result
    if isinstance(result, dict):
        for val in result.values():
            if isinstance(val, list) and val:
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


# ─── 캐시 ────────────────────────────────────────────

def load_aum_cache() -> dict:
    if os.path.exists(AUM_CACHE_FILE):
        with open(AUM_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_aum_cache(cache: dict):
    with open(AUM_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def prune_cache(cache: dict, keep_days: int = 14) -> dict:
    cutoff = (datetime.today() - timedelta(days=keep_days)).strftime("%Y%m%d")
    return {d: v for d, v in cache.items() if d >= cutoff}


# ─── KIS 공통 ────────────────────────────────────────

def get_kis_token() -> str:
    try:
        r = requests.post(
            f"{KIS_BASE_URL}/oauth2/tokenP",
            json={"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET},
            timeout=10,
        )
        return r.json().get("access_token", "")
    except Exception as e:
        log(f"  KIS 토큰 오류: {e}")
        return ""


def kis_get(path: str, tr_id: str, params: dict, token: str) -> dict:
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": tr_id,
    }
    try:
        r = requests.get(f"{KIS_BASE_URL}{path}", headers=headers, params=params, timeout=10)
        return r.json()
    except Exception as e:
        log(f"  KIS 호출 오류 ({tr_id}): {e}")
        return {}


# ─── KRX API ─────────────────────────────────────────

def get_etf_universe(client: KRXOpenAPI, base_date: str, retry: int = 1) -> dict:
    log("ETF 유니버스 수집 중...")
    df = pd.DataFrame()
    for attempt in range(retry + 1):
        try:
            df = to_df(client.get_etf_daily_trade(bas_dd=base_date))
            if not df.empty:
                break
        except Exception as e:
            log(f"  → ETF 조회 오류 (시도 {attempt+1}/{retry+1}): {e}")
            if attempt < retry:
                log("  → 5초 후 재시도...")
                time.sleep(5)
            else:
                return {}

    code_col = find_col(df, ["ISU_CD", "ISU_SRT_CD"])
    name_col = find_col(df, ["ISU_NM"])
    cap_col  = find_col(df, ["MKTCAP"])
    if not code_col or not name_col:
        return {}

    etf_info = {}
    excluded = []
    for _, row in df.iterrows():
        try:
            ticker = str(row[code_col]).strip().zfill(6)
            name   = str(row[name_col]).strip()
            mktcap = float(str(row[cap_col]).replace(",", "") or 0) if cap_col else 0
            if len(ticker) == 6 and ticker.isdigit():
                if is_valid_etf(name):
                    etf_info[ticker] = {"name": name, "mktcap": mktcap}
                else:
                    excluded.append(name)
        except:
            continue

    log(f"  → 유효 ETF {len(etf_info)}개 (제외: {len(excluded)}개)")
    bond_ex = [n for n in excluded if any(kw in n for kw in ["국고채", "채권", "금리", "10년", "30년"])]
    if bond_ex:
        log(f"  → 채권 관련 제외 예시: {bond_ex[:5]}")
    return etf_info


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
        except Exception as e:
            log(f"  → 주식 조회 오류: {e}")
    log(f"  → 주식 종목 {len(stock_info)}개 수집")
    return stock_info


# ─── KIS ETF 데이터 수집 ──────────────────────────────

def get_etf_data_today(etf_tickers: list, token: str) -> dict:
    result = {}
    for i, ticker in enumerate(etf_tickers):
        data = kis_get(
            "/uapi/etfetn/v1/quotations/inquire-price", "FHPST02400000",
            {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}, token,
        )
        output = data.get("output", {})
        try:
            lstn_stcn = float(str(output.get("lstn_stcn", "0") or "0").replace(",", ""))
            nav_raw   = float(str(output.get("nav", "0") or "0").replace(",", ""))
            cnt       = int(float(str(output.get("etf_cnfg_issu_cnt", "0") or "0").replace(",", "")))
            if lstn_stcn > 0 and nav_raw > 0:
                result[ticker] = {"lstn_stcn": lstn_stcn, "nav": nav_raw, "cnt": cnt}
        except:
            pass
        if (i + 1) % 5 == 0:
            time.sleep(1.1)
    log(f"  → ETF 데이터 수집: {len(result)}개")
    return result


# ─── KIS ETF 구성종목 ─────────────────────────────────

def get_etf_components_kis(etf_ticker: str, token: str) -> list:
    data = kis_get(
        "/uapi/etfetn/v1/quotations/inquire-component-stock-price", "FHKST121600C0",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": etf_ticker, "FID_COND_SCR_DIV_CODE": "11216"}, token,
    )
    holdings = []
    for row in data.get("output2", []):
        code   = str(row.get("stck_shrn_iscd", "")).strip().zfill(6)
        name   = str(row.get("hts_kor_isnm", "")).strip()
        weight = float(str(row.get("etf_cnfg_issu_rlim", 0) or 0))
        if len(code) == 6 and code.isdigit() and weight > 0:
            holdings.append({"ticker": code, "name": name, "weight": weight})
    return holdings


# ─── KIS 투자자별 순매수 ──────────────────────────────

def get_investor_net_buy_daily(ticker: str, token: str, display_days: int) -> dict:
    """
    FHKST01010900 - output[0]=당일, output[1]=전일 ...
    - display_days: 표시용 (요일 기준 N영업일)
    - 강도 계산용 5일치는 prsn_5d_avg에 별도 저장
    - frgn==0 and prsn==0 동시 → 장 미개장 → 제외
    - 단위: 백만원 × 1,000,000 = 원
    """
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-investor", "FHKST01010900",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}, token,
    )
    output = data.get("output", [])
    collect_days = max(display_days, PRSN_INTENSITY_DAYS)

    daily_all = []
    for row in output[:collect_days]:
        try:
            date_raw = str(row.get("stck_bsop_date", ""))
            date_str = f"{date_raw[4:6]}/{date_raw[6:8]}" if len(date_raw) == 8 else "??"
            frgn = float(str(row.get("frgn_ntby_tr_pbmn", 0) or 0)) * 1_000_000
            prsn = float(str(row.get("prsn_ntby_tr_pbmn", 0) or 0)) * 1_000_000
            if frgn == 0 and prsn == 0:
                continue
            daily_all.append({"date": date_str, "frgn": frgn, "prsn": prsn})
        except:
            continue

    daily_display = daily_all[:display_days]
    frgn_sum = sum(r["frgn"] for r in daily_display)
    prsn_sum = sum(r["prsn"] for r in daily_display)

    # 강도 계산용: 5일 일평균
    daily_5d    = daily_all[:PRSN_INTENSITY_DAYS]
    prsn_5d_avg = sum(r["prsn"] for r in daily_5d) / PRSN_INTENSITY_DAYS if daily_5d else None

    return {
        "daily":       daily_display,
        "frgn_sum":    frgn_sum,
        "prsn_sum":    prsn_sum,
        "prsn_5d_avg": prsn_5d_avg,
    }


# ─── KIS 이격도 + 거래대금 ────────────────────────────────

def get_disparity_and_volume(ticker: str, token: str, n: int = DISPARITY_PERIOD) -> dict:
    """
    FHKST01010400 - 이격도 + 거래대금 단일 호출
    거래대금 = stck_clpr × acml_vol (.replace(",","") 필수)
    output[1:]부터 사용 (output[0]=당일 acml_vol=0 이슈)
    """
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-price", "FHKST01010400",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker,
         "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"}, token,
    )
    output      = data.get("output", [])
    disparity   = 0.0
    vol_5d_avg  = 0.0
    vol_20d_avg = 0.0

    if not output:
        return {"disparity": disparity, "vol_5d_avg": vol_5d_avg, "vol_20d_avg": vol_20d_avg}

    try:
        if len(output) >= n + 1:
            prices = [
                float(str(row.get("stck_clpr", 0) or 0).replace(",", ""))
                for row in output[:n + 1]
            ]
            if not any(p == 0 for p in prices):
                disparity = (prices[0] / (sum(prices[:n]) / n) - 1) * 100

        def calc_vol(row):
            try:
                clpr = float(str(row.get("stck_clpr", 0) or 0).replace(",", ""))
                vol  = float(str(row.get("acml_vol", 0) or 0).replace(",", ""))
                return clpr * vol
            except:
                return 0.0

        confirmed = output[1:]
        vols_5  = [calc_vol(r) for r in confirmed[:5]  if calc_vol(r) > 0]
        vols_20 = [calc_vol(r) for r in confirmed[:20] if calc_vol(r) > 0]
        if vols_5:  vol_5d_avg  = sum(vols_5)  / len(vols_5)
        if vols_20: vol_20d_avg = sum(vols_20) / len(vols_20)
    except:
        pass

    return {"disparity": disparity, "vol_5d_avg": vol_5d_avg, "vol_20d_avg": vol_20d_avg}


# ─── 유틸 ────────────────────────────────────────────

def fmt(n: float) -> str:
    if abs(n) >= 1e12: return f"{n/1e12:.1f}조"
    if abs(n) >= 1e8:  return f"{n/1e8:.0f}억"
    if abs(n) >= 1e4:  return f"{n/1e4:.0f}만"
    return f"{n:,.0f}"


def fmt_flow(n: float) -> str:
    sign = "+" if n >= 0 else ""
    return f"{sign}{fmt(n)}"


def fmt_pct(v) -> str:
    """수급강도 % 포맷 (None이면 N/A)"""
    if v is None:
        return "N/A"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}%"


def fmt_investor_daily(daily: list, frgn_sum: float, prsn_sum: float) -> str:
    if not daily:
        return "  외인/개인: 데이터 없음"
    rows = list(reversed(daily))
    sep = " | "
    frgn_parts = sep.join(r["date"] + " " + fmt_flow(r["frgn"]) for r in rows)
    prsn_parts = sep.join(r["date"] + " " + fmt_flow(r["prsn"]) for r in rows)
    return (
        f"  외인  {frgn_parts} | 합산 {fmt_flow(frgn_sum)}\n"
        f"  개인  {prsn_parts} | 합산 {fmt_flow(prsn_sum)}"
    )


def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN:
        print(text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML"}, timeout=10)
            time.sleep(0.5)
        except Exception as e:
            log(f"텔레그램 오류: {e}")


# ─── COLLECT ─────────────────────────────────────────

def run_collect(etf_info: dict, token: str, base_date: str) -> bool:
    log("─" * 40)
    log(f"[COLLECT] 기준일: {base_date}")
    if not etf_info:
        log("ETF 유니버스 없음 → COLLECT 스킵")
        return False

    log(f"ETF 데이터 수집 중... ({len(etf_info)}개)")
    today_data   = get_etf_data_today(list(etf_info.keys()), token)
    concentrated = sum(1 for v in today_data.values() if v["cnt"] <= MAX_ETF_STOCKS)
    log(f"  → 집중형 (≤{MAX_ETF_STOCKS}종목): {concentrated}개 / 전체 {len(today_data)}개")

    cache = load_aum_cache()
    cache[base_date] = today_data
    cache = prune_cache(cache, keep_days=14)
    save_aum_cache(cache)
    log(f"캐시 저장 완료 | 보유 날짜: {sorted(cache.keys())}")
    return True


# ─── ANALYZE ─────────────────────────────────────────

def run_analyze(etf_info: dict, stock_info: dict, token: str, base_date: str) -> bool:
    log("─" * 40)
    log("[ANALYZE] ETF 수급 스크리너 v5.13 분석")

    cutoff = (datetime.strptime(base_date, "%Y%m%d") - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    cache  = load_aum_cache()
    available_dates = sorted([d for d in cache.keys() if d >= cutoff and d <= base_date])
    log(f"캐시 보유 날짜: {available_dates} ({len(available_dates)}일치)")

    if len(available_dates) < 2:
        log("캐시 데이터 부족 (최소 2일치) → ANALYZE 스킵")
        return False

    first_data = cache[available_dates[0]]
    last_data  = cache[available_dates[-1]]
    period_str = (
        f"{available_dates[0][4:6]}/{available_dates[0][6:]} ~ "
        f"{available_dates[-1][4:6]}/{available_dates[-1][6:]}"
    )

    etf_inflow   = {}
    filtered_cnt = 0
    for ticker in set(first_data.keys()) | set(last_data.keys()):
        f = first_data.get(ticker, {})
        l = last_data.get(ticker, {})
        stcn_change = l.get("lstn_stcn", 0) - f.get("lstn_stcn", 0)
        nav_t = l.get("nav", 0)
        cnt   = l.get("cnt", 999)
        if stcn_change <= 0 or nav_t <= 0:
            continue
        if cnt > MAX_ETF_STOCKS:
            filtered_cnt += 1
            continue
        etf_inflow[ticker] = stcn_change * nav_t

    log(f"순유입 집중형 ETF: {len(etf_inflow)}개 (지수형 제외: {filtered_cnt}개)")

    top_etfs = sorted(
        [(t, v) for t, v in etf_inflow.items() if t in etf_info],
        key=lambda x: x[1], reverse=True,
    )[:TOP_ETF_N]

    log(f"상위 {len(top_etfs)}개 집중형 ETF 선정")
    for i, (t, v) in enumerate(top_etfs[:10], 1):
        cnt  = last_data.get(t, {}).get("cnt", "?")
        name = etf_info.get(t, {}).get("name", t)
        log(f"  {i}. {name} ({t}) | 구성{cnt}종목 | +{fmt(v)}")

    if not top_etfs:
        log("선정된 ETF 없음")
        return False

    log("\n편입종목 역추적 중...")
    stock_inflow = {}
    pdf_ok = 0
    for etf_ticker, inflow in top_etfs:
        holdings = get_etf_components_kis(etf_ticker, token)
        if not holdings:
            time.sleep(0.2)
            continue
        pdf_ok += 1
        total_wt = sum(h["weight"] for h in holdings)
        for h in holdings:
            wt = h["weight"] / total_wt if total_wt > 0 else 0
            stock_inflow[h["ticker"]] = stock_inflow.get(h["ticker"], 0) + inflow * wt
        time.sleep(0.15)
    log(f"  → PDF 성공: {pdf_ok}/{len(top_etfs)} | 집계 종목: {len(stock_inflow)}개")
    if pdf_ok == 0:
        return False

    candidates   = []
    mktcap_filtered = 0
    for ticker, inflow in stock_inflow.items():
        if inflow < MIN_STOCK_INFLOW:
            continue
        info = stock_info.get(ticker, {})
        if not info.get("name"):
            continue
        mktcap = info.get("mktcap", 0)
        if mktcap < MIN_MKTCAP:
            mktcap_filtered += 1
            continue
        candidates.append({
            "ticker": ticker,
            "name":   info["name"],
            "inflow": inflow,
            "mktcap": mktcap,
        })
    log(f"  → 시총 {fmt(MIN_MKTCAP)} 미만 제외: {mktcap_filtered}개 | 후보: {len(candidates)}개")
    candidates.sort(key=lambda x: x["inflow"], reverse=True)
    top_candidates = candidates[:CANDIDATE_N]

    investor_days = get_investor_days()
    weekday_name  = ["월", "화", "수", "목", "금"][datetime.today().weekday()]
    liq_label     = fmt(MIN_LIQUIDITY_20D)
    log(f"\n이격도({DISPARITY_PERIOD}일) + 거래대금(5/20일) + 투자자({investor_days}일 표시/{PRSN_INTENSITY_DAYS}일 강도) 수집 중...")

    results      = []
    liq_filtered = 0
    for c in top_candidates:
        ticker     = c["ticker"]
        price_data = get_disparity_and_volume(ticker, token)
        disp        = price_data["disparity"]
        vol_5d_avg  = price_data["vol_5d_avg"]   # 5일 일평균 거래대금
        vol_20d_avg = price_data["vol_20d_avg"]

        if vol_20d_avg > 0 and vol_20d_avg < MIN_LIQUIDITY_20D:
            liq_filtered += 1
            time.sleep(0.05)
            continue

        # ETF수급강도(%) = ETF유입기여 / 거래대금 5일 일평균 × 100
        etf_intensity_pct = (c["inflow"] / vol_5d_avg * 100) if vol_5d_avg > 0 else None

        investor    = get_investor_net_buy_daily(ticker, token, display_days=investor_days)
        frgn_sum    = investor["frgn_sum"]
        prsn_sum    = investor["prsn_sum"]
        prsn_5d_avg = investor["prsn_5d_avg"]

        # 개인수급강도(%) = 개인순매수 5일 일평균 / 거래대금 5일 일평균 × 100
        prsn_intensity_pct = (prsn_5d_avg / vol_5d_avg * 100) if (prsn_5d_avg is not None and vol_5d_avg > 0) else None

        # 수급주체 태그
        subj_tags = []
        if frgn_sum > 0:
            subj_tags.append("🌐외국인↑")
        if prsn_sum > 0:
            subj_tags.append("👤개인↑")

        results.append({
            **c,
            "investor":           investor,
            "frgn_sum":           frgn_sum,
            "prsn_sum":           prsn_sum,
            "disp":               disp,
            "vol_5d_avg":         vol_5d_avg,
            "etf_intensity_pct":  etf_intensity_pct,
            "prsn_intensity_pct": prsn_intensity_pct,
            "subj_tags":          " ".join(subj_tags),
        })
        time.sleep(0.15)

    log(f"  → 유동성 필터 제외: {liq_filtered}개 (20일평균 < {liq_label})")

    # ETF수급강도 내림차순 정렬 (None은 뒤로)
    results.sort(key=lambda x: x["etf_intensity_pct"] if x["etf_intensity_pct"] is not None else -999, reverse=True)
    top = results[:TOP_N]
    log(f"\n  → 최종 선별: {len(top)}개")

    # ── 텔레그램 발송 ─────────────────────────────────────
    divider = "─" * 20
    now = datetime.now().strftime("%Y/%m/%d %H:%M")
    msg  = f"📊 <b>ETF 수급 종목 스크리너 v5.13</b>\n"
    msg += f"🗓 {now}  |  분석: {period_str} ({len(available_dates)}일)\n"
    msg += f"📌 집중형 ETF {len(top_etfs)}개 순유입 → <b>{len(top)}개 종목</b>\n"
    msg += f"💧 유동성 {liq_label} 미만 제외 {liq_filtered}개  |  시총 1조 미만 제외 {mktcap_filtered}개\n"
    msg += f"외인/개인: {weekday_name}요일 기준 {investor_days}영업일\n"
    msg += "━" * 24 + "\n\n"

    if not top:
        msg += "조건 충족 종목이 없습니다.\n"
    else:
        for i, r in enumerate(top, 1):
            cap_str      = fmt(r["mktcap"])
            disp_str     = f"{r['disp']:+.1f}%" if r["disp"] != 0 else "N/A"
            disp_icon    = "📉" if r["disp"] < DISPARITY_THRESH else ("📈" if r["disp"] > 2.0 else "")
            etf_pct_str  = fmt_pct(r["etf_intensity_pct"])
            prsn_pct_str = fmt_pct(r["prsn_intensity_pct"])
            subj_str     = f"  수급주체: {r['subj_tags']}\n" if r["subj_tags"] else ""
            investor_str = fmt_investor_daily(r["investor"]["daily"], r["frgn_sum"], r["prsn_sum"])

            msg += (
                f"<b>{i}. {r['name']} ({r['ticker']})</b>\n"
                f"  🏢 시총: {cap_str}\n"
                f"  💰 ETF유입: {fmt(r['inflow'])}  |  📊 이격도: {disp_str} {disp_icon}\n"
                f"  ⚡ ETF수급강도: {etf_pct_str}  👤 개인수급강도: {prsn_pct_str}\n"
                f"     (ETF유입·개인순매수 / 거래대금 5일 일평균)\n"
                f"{subj_str}"
                f"{investor_str}\n"
                f"{divider}\n\n"
            )

    send_telegram(msg)
    return True


# ─── 메인 ────────────────────────────────────────────

def main():
    if not KRX_API_KEY:
        log("KRX_API_KEY 없음. 종료.")
        return
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        log("KIS_APP_KEY/SECRET 없음. 종료.")
        return

    base_date = get_recent_business_day(1)
    log("=" * 50)
    log(f"ETF 수급 스크리너 v5.13 | 기준일: {base_date}")
    log("=" * 50)

    log("KIS 토큰 발급 중...")
    token = get_kis_token()
    if not token:
        log("KIS 토큰 실패. 종료.")
        return
    log("  → 토큰 발급 성공")

    krx = KRXOpenAPI(api_key=KRX_API_KEY, rate_limit=5, per_seconds=1)

    log("\nKRX 데이터 수집 중...")
    etf_info   = get_etf_universe(krx, base_date, retry=1)
    stock_info = get_stock_info_bulk(krx, base_date)

    if not etf_info:
        log("ETF 유니버스 수집 실패. 종료.")
        return

    collect_ok = run_collect(etf_info, token, base_date)
    if not collect_ok:
        log("COLLECT 실패. ANALYZE 스킵.")
        return

    log("")
    run_analyze(etf_info, stock_info, token, base_date)
    log("\n완료!")


if __name__ == "__main__":
    main()
