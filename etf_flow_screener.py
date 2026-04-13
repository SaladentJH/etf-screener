"""
ETF 수급 기반 종목 스크리너 v5.2
미래에셋증권 "신(新) 수급의 시대" 전략 구현

변경 (v5.1 → v5.2):
  - ETF 순유입 계산 방식 개선 (이호준님 조언)
    v5.1: AUM(T) - AUM(T-1)
          → 가격 상승분 포함, 실제 자금 유입 구분 불가
    v5.2: (상장주수(T) - 상장주수(T-1)) × NAV(T)
          → 실제 CU 설정/환매에 의한 순유입만 측정
          → 가격 변화에 의한 AUM 변화 제거

  - COLLECT: lstn_stcn(상장주수) + nav 추가 수집
  - ANALYZE: 상장주수 변화 × NAV = ETF 실제 순유입

기존 유지:
  - 집중형 ETF 필터: 구성종목 수 30개 이하 (MAX_ETF_STOCKS)
  - 이격도: 20일 (DISPARITY_PERIOD)
  - 외국인/기관 순매수: 개별종목 보조 태그 (단위: 백만원 × 1,000,000)
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
RUN_MODE           = os.environ.get("MODE", "")   # COLLECT / ANALYZE / 빈값(자동)

KIS_BASE_URL   = "https://openapi.koreainvestment.com:9443"
AUM_CACHE_FILE = "etf_aum_cache.json"

# ─── 분석 파라미터 ─────────────────────────────────────
MIN_STOCK_INFLOW = 3_000_000_000   # 종목 최소 ETF 유입 기여액 (3억)
TOP_ETF_N        = 30              # 주간 순유입 상위 ETF 수
CANDIDATE_N      = 30              # KIS 조회 후보 종목 수
TOP_N            = 30              # 최종 발송 종목 수
LOOKBACK_DAYS    = 7               # 주간 분석 기간 (캘린더 기준)
DISPARITY_PERIOD = 20              # 이격도 이동평균 기간 (미래에셋 리포트: 20일)
DISPARITY_THRESH = -2.0            # 이격도 단기하락 기준 (%)
MAX_ETF_STOCKS   = 30              # 집중형 ETF 기준: 구성종목 수 30개 이하만 허용

EXCLUDE_KEYWORDS = [
    "레버리지", "인버스", "2X", "3X", "-1X", "곱버스",
    "해외", "미국", "중국", "일본", "인도", "베트남", "나스닥", "S&P",
    "채권", "국채", "달러", "금", "선물", "WTI", "유가", "원유",
    "커버드콜", "covered", "프리미엄", "위클리",
    "머니마켓", "MMF", "단기", "CD금리", "KOFR", "SOFR",
    "부동산", "리츠", "REIT",
]
# ──────────────────────────────────────────────────────


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def is_valid_etf(name: str) -> bool:
    for kw in EXCLUDE_KEYWORDS:
        if kw in name:
            return False
    return True


def get_recent_business_day(days_back: int = 1) -> str:
    date = datetime.today() - timedelta(days=days_back)
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    return date.strftime("%Y%m%d")


def is_friday() -> bool:
    return datetime.today().weekday() == 4


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


# ─── 캐시 구조 ───────────────────────────────────────
# {
#   "날짜": {
#     "ticker": {
#       "lstn_stcn": float,   # 상장주수 (CU 설정/환매 감지)
#       "nav":       float,   # NAV (원) — 순유입 금액 환산
#       "cnt":       int,     # 구성종목 수 — 집중형 필터
#     }
#   }
# }

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
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    try:
        r = requests.post(url, json=body, timeout=10)
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


# ─── KIS ETF 데이터 수집 ──────────────────────────────

def get_etf_data_today(etf_tickers: list, token: str) -> dict:
    """
    FHPST02400000 (ETF/ETN 현재가)

    수집 필드:
      lstn_stcn       : 상장 주수 → 전일 대비 변화가 실제 CU 설정/환매량
      nav             : NAV (원) → 상장주수 변화 × NAV = 실제 순유입 금액
      etf_cnfg_issu_cnt: 구성종목 수 → 집중형 필터 (≤ MAX_ETF_STOCKS)

    반환: {ticker: {"lstn_stcn": float, "nav": float, "cnt": int}}

    순유입 계산 (ANALYZE 단계):
      ETF 순유입 = (lstn_stcn(T) - lstn_stcn(T-1)) × nav(T)
      → 가격 변동분 제거, 실제 자금 유입/유출만 반영
    """
    result = {}
    for i, ticker in enumerate(etf_tickers):
        data = kis_get(
            "/uapi/etfetn/v1/quotations/inquire-price",
            "FHPST02400000",
            {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            token,
        )
        output = data.get("output", {})
        try:
            lstn_stcn = float(str(output.get("lstn_stcn", "0") or "0").replace(",", ""))
            nav_raw   = float(str(output.get("nav", "0") or "0").replace(",", ""))
            cnt       = int(float(str(output.get("etf_cnfg_issu_cnt", "0") or "0").replace(",", "")))

            if lstn_stcn > 0 and nav_raw > 0:
                result[ticker] = {
                    "lstn_stcn": lstn_stcn,
                    "nav":       nav_raw,
                    "cnt":       cnt,
                }
        except:
            pass

        # Rate limit: 초당 5회 이하
        if (i + 1) % 5 == 0:
            time.sleep(1.1)

    log(f"  → ETF 데이터 수집: {len(result)}개")
    return result


# ─── KIS ETF 구성종목 ─────────────────────────────────

def get_etf_components_kis(etf_ticker: str, token: str) -> list:
    data = kis_get(
        "/uapi/etfetn/v1/quotations/inquire-component-stock-price",
        "FHKST121600C0",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": etf_ticker, "FID_COND_SCR_DIV_CODE": "11216"},
        token,
    )
    holdings = []
    for row in data.get("output2", []):
        code   = str(row.get("stck_shrn_iscd", "")).strip().zfill(6)
        name   = str(row.get("hts_kor_isnm", "")).strip()
        weight = float(str(row.get("etf_cnfg_issu_rlim", 0) or 0))
        if len(code) == 6 and code.isdigit() and weight > 0:
            holdings.append({"ticker": code, "name": name, "weight": weight})
    return holdings


# ─── KIS 투자자별 순매수 (개별종목 보조 지표) ────────────

def get_investor_net_buy(ticker: str, token: str) -> dict:
    """
    FHKST01010900 - 주식현재가 투자자
    개별 종목 외국인/기관 순매수 (보조 태그용)
    단위: 백만원 × 1,000,000 = 원
    """
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-investor",
        "FHKST01010900",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
        token,
    )
    output = data.get("output", [])
    if not output:
        return {"frgn": 0, "orgn": 0}
    row = output[0]
    try:
        frgn = float(str(row.get("frgn_ntby_tr_pbmn", 0) or 0)) * 1_000_000
        orgn = float(str(row.get("orgn_ntby_tr_pbmn", 0) or 0)) * 1_000_000
        return {"frgn": frgn, "orgn": orgn}
    except:
        return {"frgn": 0, "orgn": 0}


# ─── KIS 이격도 계산 (20일) ───────────────────────────

def get_disparity(ticker: str, token: str, n: int = DISPARITY_PERIOD) -> float:
    """
    FHKST01010400 - 일별 종가 기반 n일 이격도
    현재가 / n일 이동평균 - 1
    기본값: 20일 (미래에셋 리포트 원본 기준)
    """
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
        "FHKST01010400",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker,
         "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"},
        token,
    )
    output = data.get("output", [])
    if len(output) < n + 1:
        return 0.0
    try:
        prices = [float(str(row.get("stck_clpr", 0) or 0)) for row in output[:n + 1]]
        if any(p == 0 for p in prices):
            return 0.0
        current = prices[0]
        ma_n    = sum(prices[:n]) / n
        return (current / ma_n - 1) * 100
    except:
        return 0.0


# ─── KRX API ─────────────────────────────────────────

def get_etf_universe(client: KRXOpenAPI, base_date: str) -> dict:
    """유효 국내주식형 ETF 목록 {ticker: {name, mktcap}}"""
    log("ETF 유니버스 수집 중...")
    try:
        df = to_df(client.get_etf_daily_trade(bas_dd=base_date))
        if df.empty:
            return {}
    except Exception as e:
        log(f"  → ETF 조회 오류: {e}")
        return {}

    code_col = find_col(df, ["ISU_CD", "ISU_SRT_CD"])
    name_col = find_col(df, ["ISU_NM"])
    cap_col  = find_col(df, ["MKTCAP"])
    if not code_col or not name_col:
        return {}

    etf_info = {}
    for _, row in df.iterrows():
        try:
            ticker = str(row[code_col]).strip().zfill(6)
            name   = str(row[name_col]).strip()
            mktcap = float(str(row[cap_col]).replace(",", "") or 0) if cap_col else 0
            if len(ticker) == 6 and ticker.isdigit() and is_valid_etf(name):
                etf_info[ticker] = {"name": name, "mktcap": mktcap}
        except:
            continue

    log(f"  → 유효 ETF {len(etf_info)}개")
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
        except:
            continue
    log(f"  → 주식 종목 {len(stock_info)}개 수집")
    return stock_info


# ─── 유틸 ────────────────────────────────────────────

def fmt(n: float) -> str:
    if abs(n) >= 1e12: return f"{n/1e12:.1f}조"
    if abs(n) >= 1e8:  return f"{n/1e8:.0f}억"
    if abs(n) >= 1e4:  return f"{n/1e4:.0f}만"
    return f"{n:,.0f}"


def fmt_flow(n: float) -> str:
    sign = "+" if n >= 0 else ""
    return f"{sign}{fmt(n)}"


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


# ─── COLLECT 모드 ─────────────────────────────────────

def run_collect():
    """
    매 영업일 장마감 후 실행.
    ETF별 상장주수(lstn_stcn) + NAV + 구성종목수 수집 → 캐시 저장.
    """
    log("=" * 50)
    log("[COLLECT 모드] ETF 상장주수 + NAV + 구성종목수 수집")
    log("=" * 50)

    base_date = get_recent_business_day(1)
    log(f"기준일: {base_date}")

    krx = KRXOpenAPI(api_key=KRX_API_KEY, rate_limit=5, per_seconds=1)
    etf_info = get_etf_universe(krx, base_date)
    if not etf_info:
        log("ETF 유니버스 조회 실패")
        return

    log("KIS 토큰 발급 중...")
    token = get_kis_token()
    if not token:
        log("KIS 토큰 실패")
        return
    log("  → 토큰 발급 성공")

    log(f"ETF 데이터 수집 중... ({len(etf_info)}개)")
    today_data = get_etf_data_today(list(etf_info.keys()), token)

    # 집중형 ETF 비율 로그
    concentrated = sum(1 for v in today_data.values() if v["cnt"] <= MAX_ETF_STOCKS)
    log(f"  → 집중형 (≤{MAX_ETF_STOCKS}종목): {concentrated}개 / 전체 {len(today_data)}개")

    cache = load_aum_cache()
    cache[base_date] = today_data
    cache = prune_cache(cache, keep_days=14)
    save_aum_cache(cache)

    log(f"캐시 저장 완료 → {AUM_CACHE_FILE}")
    log(f"저장된 날짜: {sorted(cache.keys())}")
    log("완료!")


# ─── ANALYZE 모드 ─────────────────────────────────────

def run_analyze():
    """
    금요일 장마감 후 실행.

    ETF 실제 순유입 계산:
      inflow = (lstn_stcn(last) - lstn_stcn(first)) × nav(last)
      → 상장주수 증가 = CU 설정 발생 = 실제 자금 유입
      → 가격 상승에 의한 AUM 증가는 반영 안 됨

    이후: 집중형 ETF 필터 → 편입종목 역추적 → 발송
    """
    log("=" * 50)
    log("[ANALYZE 모드] ETF 수급 스크리너 v5.2 분석")
    log("=" * 50)

    base_date = get_recent_business_day(1)
    cutoff_dt = datetime.strptime(base_date, "%Y%m%d") - timedelta(days=LOOKBACK_DAYS)
    cutoff    = cutoff_dt.strftime("%Y%m%d")
    log(f"분석기간: {cutoff} ~ {base_date}")

    # ── 캐시에서 주간 순유입 계산 ──────────────────────
    cache = load_aum_cache()
    available_dates = sorted([d for d in cache.keys() if d >= cutoff and d <= base_date])
    log(f"캐시 보유 날짜: {available_dates}")

    if len(available_dates) < 2:
        log("캐시 데이터 부족 (최소 2일치 필요). COLLECT 모드를 먼저 실행하세요.")
        send_telegram("❌ ETF 스크리너 v5.2: 캐시 데이터 부족. 매일 COLLECT 실행 필요.")
        return

    first_data = cache[available_dates[0]]   # {ticker: {lstn_stcn, nav, cnt}}
    last_data  = cache[available_dates[-1]]

    # 실제 순유입 = 상장주수 변화 × NAV (가격 변동분 제거)
    etf_inflow = {}
    filtered_cnt = 0
    for ticker in set(first_data.keys()) | set(last_data.keys()):
        f = first_data.get(ticker, {})
        l = last_data.get(ticker, {})

        stcn_change = l.get("lstn_stcn", 0) - f.get("lstn_stcn", 0)
        nav_t       = l.get("nav", 0)
        cnt         = l.get("cnt", 999)

        if stcn_change <= 0 or nav_t <= 0:
            continue
        if cnt > MAX_ETF_STOCKS:   # 지수형 제외
            filtered_cnt += 1
            continue

        inflow = stcn_change * nav_t   # 실제 유입 금액 (원)
        etf_inflow[ticker] = inflow

    log(f"주간 순유입 집중형 ETF: {len(etf_inflow)}개 (지수형 제외: {filtered_cnt}개)")

    # ── ETF 유니버스 (이름 조회용) ────────────────────────
    krx = KRXOpenAPI(api_key=KRX_API_KEY, rate_limit=5, per_seconds=1)
    etf_info = get_etf_universe(krx, base_date)

    top_etfs = sorted(
        [(t, v) for t, v in etf_inflow.items() if t in etf_info],
        key=lambda x: x[1], reverse=True
    )[:TOP_ETF_N]

    log(f"상위 {len(top_etfs)}개 집중형 ETF 선정")
    for i, (t, v) in enumerate(top_etfs[:5], 1):
        cnt = last_data.get(t, {}).get("cnt", "?")
        log(f"  {i}. {etf_info.get(t, {}).get('name', t)} ({t}) | 구성{cnt}종목 | +{fmt(v)}")

    if not top_etfs:
        send_telegram("❌ ETF 스크리너 v5.2: 조건 충족 집중형 ETF 없음")
        return

    # ── KIS 토큰 ──────────────────────────────────────────
    log("\nKIS 토큰 발급 중...")
    token = get_kis_token()
    if not token:
        send_telegram("❌ ETF 스크리너 v5.2: KIS 토큰 실패")
        return
    log("  → 토큰 발급 성공")

    # ── 편입종목 역추적 ──────────────────────────────────
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
            wt      = h["weight"] / total_wt if total_wt > 0 else 0
            contrib = inflow * wt   # ETF 순유입 × 편입비중
            stock_inflow[h["ticker"]] = stock_inflow.get(h["ticker"], 0) + contrib
        time.sleep(0.15)

    log(f"  → PDF 성공: {pdf_ok}/{len(top_etfs)} | 집계 종목: {len(stock_inflow)}개")

    if pdf_ok == 0:
        send_telegram("❌ ETF 스크리너 v5.2: 편입종목 조회 실패")
        return

    # ── 종목 정보 수집 ────────────────────────────────────
    log("\n종목 정보 수집 중...")
    stock_info = get_stock_info_bulk(krx, base_date)

    candidates = []
    for ticker, inflow in stock_inflow.items():
        if inflow < MIN_STOCK_INFLOW:
            continue
        info   = stock_info.get(ticker, {})
        name   = info.get("name", "")
        mktcap = info.get("mktcap", 0)
        if not name:
            continue
        candidates.append({"ticker": ticker, "name": name, "inflow": inflow, "mktcap": mktcap})

    candidates.sort(key=lambda x: x["inflow"], reverse=True)
    top_candidates = candidates[:CANDIDATE_N]
    log(f"  → 후보 종목: {len(top_candidates)}개")

    # ── 투자자 순매수(보조) + 이격도(20일) 수집 ─────────────
    log(f"\n투자자 순매수(보조) + 이격도({DISPARITY_PERIOD}일) 수집 중...")
    results = []

    for c in top_candidates:
        ticker   = c["ticker"]
        investor = get_investor_net_buy(ticker, token)
        frgn     = investor["frgn"]
        orgn     = investor["orgn"]
        disp     = get_disparity(ticker, token)   # 기본값 DISPARITY_PERIOD=20일

        tags = ["🎯ETF수급"]
        if c["mktcap"] > 0 and c["mktcap"] < 1_000_000_000_000:
            tags.append("🔹소형주")
        if disp < DISPARITY_THRESH:
            tags.append("📉단기하락")
        if frgn > 0:
            tags.append("🌐외국인↑")
        if orgn > 0:
            tags.append("🏦기관↑")

        results.append({**c, "frgn": frgn, "orgn": orgn, "disp": disp, "tags": " ".join(tags)})
        time.sleep(0.15)

    results.sort(key=lambda x: x["inflow"], reverse=True)
    top = results[:TOP_N]
    log(f"\n  → 최종 선별: {len(top)}개")

    # ── 텔레그램 발송 ─────────────────────────────────────
    now = datetime.now().strftime("%Y/%m/%d %H:%M")
    msg  = f"📊 <b>ETF 수급 종목 스크리너 v5.2</b> | {now}\n"
    msg += f"분석기간: {cutoff[4:6]}/{cutoff[6:]} ~ {base_date[4:6]}/{base_date[6:]} ({len(available_dates)}일)\n"
    msg += f"집중형 ETF {len(top_etfs)}개 실제 순유입 역추적 → <b>{len(top)}개 종목</b> 선별\n"
    msg += f"(순유입 = 상장주수 변화 × NAV | 구성종목 {MAX_ETF_STOCKS}개 이하)\n"
    msg += "─" * 28 + "\n\n"

    if not top:
        msg += "조건 충족 종목이 없습니다.\n"
    else:
        for i, r in enumerate(top, 1):
            cap_str   = fmt(r["mktcap"]) if r["mktcap"] > 0 else "N/A"
            disp_str  = f"{r['disp']:+.1f}%" if r["disp"] != 0 else "N/A"
            disp_icon = " 📉" if r["disp"] < DISPARITY_THRESH else (" 📈" if r["disp"] > 2.0 else "")
            msg += (
                f"<b>{i}. {r['name']} ({r['ticker']})</b>\n"
                f"  ETF유입기여: {fmt(r['inflow'])} | 이격도({DISPARITY_PERIOD}일): {disp_str}{disp_icon}\n"
                f"  외국인: {fmt_flow(r['frgn'])} | 기관: {fmt_flow(r['orgn'])}\n"
                f"  시총: {cap_str} | {r['tags']}\n\n"
            )

    send_telegram(msg)
    log("\n완료!")


# ─── 메인 ────────────────────────────────────────────

def main():
    if not KRX_API_KEY:
        log("KRX_API_KEY 없음. 종료.")
        return
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        log("KIS_APP_KEY/SECRET 없음. 종료.")
        return

    mode = RUN_MODE.upper()
    if not mode:
        mode = "ANALYZE" if is_friday() else "COLLECT"
    log(f"실행 모드: {mode}")

    if mode == "COLLECT":
        run_collect()
    elif mode == "ANALYZE":
        run_analyze()
    else:
        log(f"알 수 없는 MODE: {mode}")


if __name__ == "__main__":
    main()
