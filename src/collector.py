"""
collector.py
------------
Global Daily News — 39개 진출국 데이터 수집기 (v5)

이번 버전에서 추가/수정된 것
  A) 일별 아카이브
     - 매 실행 결과를 data/countries_data.json(작업 캐시)뿐 아니라
       docs/archive/YYYY-MM-DD.json 으로도 저장한다 (GitHub Pages가 docs/를 서빙하므로
       프론트엔드에서 fetch('./archive/2026-09-01.json')로 바로 접근 가능).
     - docs/archive/index.json 에 유효한 날짜 목록(min/max 포함)을 기록/갱신한다.
     - 180일이 지난 아카이브 파일은 저장소 용량 관리를 위해 자동 삭제한다(Retention Policy).
  B) 한국어 기사 우선 수집 → 영문 기사 대체 + 자동 번역 (2단계 파이프라인)
     - 모든 국가(한국 포함 39개국)에 대해 먼저 한국어 Google News RSS(hl=ko&gl=KR)로 검색한다.
     - 슬롯이 다 채워지지 않으면 영문 Google News(hl=en-US&gl=US)로 나머지를 채우고,
       deep-translator로 제목/요약을 한국어로 번역해 저장한다(원문은 참고용으로 함께 보관).
  C) 환율 수집을 "일별 종가 직접 조회 + 월별 자체 리샘플" 방식으로 재구성
     - 기존에는 yfinance의 월봉(interval="1mo")을 그대로 썼는데, 통화에 따라 월봉 데이터가
       비거나 성기게 나와 차트가 비어 보이는 문제(중국 CNY 등)가 있었다.
     - 이제 ~400일치 일봉을 받아 pandas로 "연-월" 단위 그룹핑 후 각 월의 마지막 종가를 뽑아
       12개월 치를 만든다. 직접 페어({통화}KRW=X)가 부실하면(30거래일 미만) 자동으로
       USD 경유 교차 환산({통화}USD=X × USDKRW=X)으로 전환한다.
  D) 소액 통화 100단위 환산
     - VND/JPY/IDR 은 1단위 환율이 반올림 시 '0원'으로 보이는 문제가 있어
       100단위 기준으로 재계산해 저장한다(unit_base 필드로 프론트에 전달).

결과는 data/countries_data.json(최신 1건, 로컬 개발/빌드용)과
docs/archive/YYYY-MM-DD.json(일별 아카이브) 두 곳에 저장된다.
"""

import os
import re
import json
import logging
from datetime import datetime, timedelta
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import feedparser
import pandas as pd
import pytz

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("collector")

KST = pytz.timezone("Asia/Seoul")
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_PATH = os.path.join(DATA_DIR, "countries_data.json")
ARCHIVE_DIR = os.path.join(BASE_DIR, "docs", "archive")
ARCHIVE_INDEX_PATH = os.path.join(ARCHIVE_DIR, "index.json")
ARCHIVE_RETENTION_DAYS = 180

MONTH_KR = ["1월", "2월", "3월", "4월", "5월", "6월",
            "7월", "8월", "9월", "10월", "11월", "12월"]

BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

REQUEST_TIMEOUT = 10
MAX_WORKERS = 8  # 국가 단위 병렬 수집 스레드 수

# 소액 통화 — 100단위 기준으로 환산해 저장 (1단위 환율이 반올림 시 '0원'이 되는 문제 방지)
SMALL_UNIT_CURRENCIES = {"VND": 100, "JPY": 100, "IDR": 100}


# ---------------------------------------------------------------------------
# 0. 39개국 메타데이터 & 권역 정의
# ---------------------------------------------------------------------------
REGIONS = [
    {"key": "ALL", "label": "전체"},
    {"key": "KR", "label": "한국"},
    {"key": "CN", "label": "중국"},
    {"key": "AMER", "label": "미주"},
    {"key": "EU", "label": "유럽"},
    {"key": "LATAM", "label": "중남미"},
    {"key": "APAC", "label": "아태"},
    {"key": "MEA", "label": "중아"},
]

# (code, region, name_kr, name_en, currency, flag, hubs[])
# code 는 World Bank / flagcdn 이 사용하는 ISO 3166-1 alpha-2 기준 (영국=GB)
_RAW_COUNTRIES = [
    ("KR", "KR", "한국", "South Korea", "KRW", "kr",
     ["본사 (판교 테크노플렉스)", "중앙연구소 (한국테크노돔)", "대전공장", "금산공장", "한국테크노링 (주행시험장)"]),
    ("CN", "CN", "중국", "China", "CNY", "cn",
     ["중국본부 (상하이)", "연구소 (CTC 가흥)", "가흥공장", "강소공장", "중경공장", "영업지사 11개"]),
    ("US", "AMER", "미국", "United States", "USD", "us",
     ["미주본부 (내슈빌)", "미국기술센터 (ATC, 오하이오)", "테네시공장 (클락스빌)"]),
    ("CA", "AMER", "캐나다", "Canada", "CAD", "ca",
     ["캐나다 판매법인 (온타리오)"]),
    ("DE", "EU", "독일", "Germany", "EUR", "de",
     ["유럽본부 (노이지젠부르크)", "유럽기술센터 (ETC, 하노버)", "독일 판매법인"]),
    ("HU", "EU", "헝가리", "Hungary", "HUF", "hu",
     ["헝가리공장 (라칼마스)", "헝가리 판매법인 (부다페스트)"]),
    ("GB", "EU", "영국", "United Kingdom", "GBP", "gb",
     ["영국 판매법인 (다번트리)"]),
    ("FR", "EU", "프랑스", "France", "EUR", "fr",
     ["프랑스 판매법인 (리옹)"]),
    ("IT", "EU", "이탈리아", "Italy", "EUR", "it",
     ["이탈리아 판매법인 (밀라노)"]),
    ("ES", "EU", "스페인", "Spain", "EUR", "es",
     ["스페인 판매법인 (마드리드)"]),
    ("PL", "EU", "폴란드", "Poland", "PLN", "pl",
     ["폴란드 판매법인 (바르샤바)"]),
    ("CZ", "EU", "체코", "Czech Republic", "CZK", "cz",
     ["체코 판매법인 (프라하)"]),
    ("NL", "EU", "네덜란드", "Netherlands", "EUR", "nl",
     ["네덜란드 판매법인 (암스테르담)"]),
    ("SE", "EU", "스웨덴", "Sweden", "SEK", "se",
     ["스웨덴 판매법인 (스톡홀름)"]),
    ("AT", "EU", "오스트리아", "Austria", "EUR", "at",
     ["오스트리아 판매법인 (비엔나)"]),
    ("RO", "EU", "루마니아", "Romania", "RON", "ro",
     ["루마니아 판매법인 (부쿠레슈티)"]),
    ("RU", "EU", "러시아", "Russia", "RUB", "ru",
     ["러시아 판매법인 (모스크바)"]),
    ("UA", "EU", "우크라이나", "Ukraine", "UAH", "ua",
     ["우크라이나 지사 (키이우)"]),
    ("TR", "EU", "튀르키예", "Turkey", "TRY", "tr",
     ["튀르키예 판매법인 (이스탄불)"]),
    ("RS", "EU", "세르비아", "Serbia", "RSD", "rs",
     ["세르비아 세일즈 오피스"]),
    ("HR", "EU", "크로아티아", "Croatia", "EUR", "hr",
     ["크로아티아 세일즈 오피스"]),
    ("MA", "EU", "모로코", "Morocco", "MAD", "ma",
     ["모로코 세일즈 오피스 (카사블랑카)"]),
    ("MX", "LATAM", "멕시코", "Mexico", "MXN", "mx",
     ["멕시코 판매법인 (멕시코시티)"]),
    ("BR", "LATAM", "브라질", "Brazil", "BRL", "br",
     ["브라질 판매법인 (상파울루)"]),
    ("CL", "LATAM", "칠레", "Chile", "CLP", "cl",
     ["칠레 판매법인 (산티아고)"]),
    ("CO", "LATAM", "콜롬비아", "Colombia", "COP", "co",
     ["콜롬비아 지사 (보고타)"]),
    ("PA", "LATAM", "파나마", "Panama", "PAB", "pa",
     ["파나마 판매법인 (파나마시티)"]),
    ("ID", "APAC", "인도네시아", "Indonesia", "IDR", "id",
     ["아태본부", "인도네시아공장 (찌카랑)", "인도네시아 판매법인"]),
    ("AU", "APAC", "호주", "Australia", "AUD", "au",
     ["호주 판매법인 (시드니)"]),
    ("JP", "APAC", "일본", "Japan", "JPY", "jp",
     ["일본기술센터 (JTC)", "일본 판매법인 (도쿄)"]),
    ("SG", "APAC", "싱가포르", "Singapore", "SGD", "sg",
     ["싱가포르 법인"]),
    ("MY", "APAC", "말레이시아", "Malaysia", "MYR", "my",
     ["말레이시아 판매법인 (쿠알라룸푸르)"]),
    ("TH", "APAC", "태국", "Thailand", "THB", "th",
     ["태국 판매법인 (방콕)"]),
    ("VN", "APAC", "베트남", "Vietnam", "VND", "vn",
     ["베트남 판매법인 (호치민)"]),
    ("TW", "APAC", "대만", "Taiwan", "TWD", "tw",
     ["대만 지사 (타이베이)"]),
    ("AE", "MEA", "아랍에미리트", "United Arab Emirates", "AED", "ae",
     ["중동본부 (두바이)"]),
    ("SA", "MEA", "사우디아라비아", "Saudi Arabia", "SAR", "sa",
     ["사우디 세일즈 오피스 (제다)"]),
    ("EG", "MEA", "이집트", "Egypt", "EGP", "eg",
     ["이집트 지사 (카이로)"]),
    ("KZ", "MEA", "카자흐스탄", "Kazakhstan", "KZT", "kz",
     ["카자흐스탄 지사 (알마티)"]),
]

COUNTRIES = [
    {"code": c, "region": r, "name_kr": nk, "name_en": ne, "currency": cur, "flag": fl, "hubs": h}
    for c, r, nk, ne, cur, fl, h in _RAW_COUNTRIES
]

CAPITAL_FALLBACK = {"TW": "Taipei"}

STATUTORY_MIN_WAGE = {
    "US": {"federal": "$7.25 / h (연방, 2009-07-24 발효)", "note": "테네시: 주 별도 기준 없음 → 연방 기준 적용"},
}


# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------
def load_cache() -> dict:
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return {c["code"]: c for c in payload.get("countries", [])}
        except Exception as e:
            log.warning(f"캐시 로드 실패: {e}")
    return {}


def save_cache(data: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_archive(data: dict) -> None:
    """docs/archive/YYYY-MM-DD.json 저장 + index.json 갱신 + 180일 보존정책 적용."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    date_str = data["generated_at"][:10]  # KST 기준 YYYY-MM-DD

    archive_path = os.path.join(ARCHIVE_DIR, f"{date_str}.json")
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)  # 용량 절약을 위해 압축(무들여쓰기) 저장

    _apply_retention_and_reindex()


def _apply_retention_and_reindex() -> None:
    cutoff = datetime.now(KST).date() - timedelta(days=ARCHIVE_RETENTION_DAYS)
    valid_dates = []

    for fname in os.listdir(ARCHIVE_DIR):
        if fname == "index.json" or not fname.endswith(".json"):
            continue
        date_part = fname[:-5]
        try:
            d = datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            continue  # 형식에 안 맞는 파일은 건드리지 않음

        if d < cutoff:
            try:
                os.remove(os.path.join(ARCHIVE_DIR, fname))
                log.info(f"[retention] {fname} 삭제 (180일 경과)")
            except Exception as e:
                log.warning(f"[retention] {fname} 삭제 실패: {e}")
            continue

        valid_dates.append(date_part)

    valid_dates.sort()
    index_payload = {
        "dates": valid_dates,
        "min_date": valid_dates[0] if valid_dates else None,
        "latest": valid_dates[-1] if valid_dates else None,
    }
    with open(ARCHIVE_INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index_payload, f, ensure_ascii=False, indent=2)


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def _shorten(text: str, max_len: int) -> str:
    text = " ".join(text.split())
    return text if len(text) <= max_len else text[:max_len].rstrip() + "…"


def _safe_get(url: str, **kwargs):
    resp = requests.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
    resp.raise_for_status()
    return resp


# ---------------------------------------------------------------------------
# 1. World Bank — 수도 / 인구 / GDP / (기본) 인플레이션·실업률
# ---------------------------------------------------------------------------
def _wb_country_meta(code: str) -> str:
    url = f"https://api.worldbank.org/v2/country/{code}?format=json"
    payload = _safe_get(url).json()
    row = payload[1][0]
    capital = row.get("capitalCity") or ""
    if not capital:
        raise ValueError("수도 정보 없음")
    return capital


def _wb_latest_value(code: str, indicator: str):
    url = f"https://api.worldbank.org/v2/country/{code}/indicator/{indicator}?format=json&per_page=5"
    payload = _safe_get(url).json()
    for row in payload[1]:
        if row.get("value") is not None:
            return float(row["value"]), row["date"]
    raise ValueError("유효 데이터 없음")


# ---------------------------------------------------------------------------
# 2. 미국 전용 — 인플레이션·실업률 BLS → FRED → World Bank 폴백
# ---------------------------------------------------------------------------
def _bls_series(series_ids: list, start_year: int, end_year: int) -> dict:
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    payload = {"seriesid": series_ids, "startyear": str(start_year), "endyear": str(end_year)}
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY
    resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError("BLS API 오류")
    return {s["seriesID"]: s["data"] for s in body["Results"]["series"]}


def _us_labor_stats_bls() -> dict:
    year = datetime.now(KST).year
    series = {"unemployment": "LNS14000000", "cpi": "CUUR0000SA0"}
    data = _bls_series(list(series.values()), year - 1, year)
    unemployment = float(data[series["unemployment"]][0]["value"])
    cpi_points = data[series["cpi"]]
    latest = cpi_points[0]
    prior = next(p for p in cpi_points if p["period"] == latest["period"] and int(p["year"]) == int(latest["year"]) - 1)
    inflation = (float(latest["value"]) - float(prior["value"])) / float(prior["value"]) * 100
    return {"inflation": f"{inflation:.1f}%", "unemployment": f"{unemployment:.1f}%",
            "stats_source": "BLS", "stats_asof": f"{latest['year']}-{latest['period'].replace('M', '')}"}


def _us_labor_stats_fred() -> dict:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY 미설정")
    url = "https://api.stlouisfed.org/fred/series/observations"

    def obs(series_id, limit):
        params = {"series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json",
                   "sort_order": "desc", "limit": limit}
        return _safe_get(url, params=params).json()["observations"]

    unemployment = float(obs("UNRATE", 1)[0]["value"])
    cpi = obs("CPIAUCSL", 13)
    inflation = (float(cpi[0]["value"]) - float(cpi[-1]["value"])) / float(cpi[-1]["value"]) * 100
    return {"inflation": f"{inflation:.1f}%", "unemployment": f"{unemployment:.1f}%",
            "stats_source": "FRED", "stats_asof": cpi[0]["date"][:7]}


def get_inflation_unemployment(code: str, cached_profile: dict) -> dict:
    if code == "US":
        for label, fn in (("BLS", _us_labor_stats_bls), ("FRED", _us_labor_stats_fred)):
            try:
                return fn()
            except Exception as e:
                log.warning(f"[US/{label}] 물가·고용 지표 실패: {e}")
    try:
        infl, infl_date = _wb_latest_value(code, "FP.CPI.TOTL.ZG")
        unemp, _ = _wb_latest_value(code, "SL.UEM.TOTL.ZS")
        return {"inflation": f"{infl:.1f}%", "unemployment": f"{unemp:.1f}%",
                "stats_source": "World Bank", "stats_asof": infl_date}
    except Exception as e:
        log.warning(f"[{code}/World Bank] 물가·고용 지표 실패 → 캐시 사용: {e}")
        return {
            "inflation": cached_profile.get("inflation", "-"),
            "unemployment": cached_profile.get("unemployment", "-"),
            "stats_source": cached_profile.get("stats_source", "캐시"),
            "stats_asof": cached_profile.get("stats_asof", "-"),
        }


def build_profile(country: dict, cached: dict) -> dict:
    code = country["code"]
    cached_profile = (cached or {}).get("profile", {})
    profile = {}

    try:
        profile["capital"] = _wb_country_meta(code)
    except Exception as e:
        log.warning(f"[{code}] 수도 정보 실패: {e}")
        profile["capital"] = CAPITAL_FALLBACK.get(code, cached_profile.get("capital", "-"))

    try:
        pop, _ = _wb_latest_value(code, "SP.POP.TOTL")
        profile["population"] = f"약 {pop / 1e8:.2f}억 명" if pop >= 1e8 else f"약 {pop / 1e4:.0f}만 명"
    except Exception as e:
        log.warning(f"[{code}] 인구 지표 실패 → 캐시 사용: {e}")
        profile["population"] = cached_profile.get("population", "-")

    try:
        gdp, _ = _wb_latest_value(code, "NY.GDP.MKTP.CD")
        profile["gdp"] = f"약 {gdp / 1e12:.2f}조 USD" if gdp >= 1e12 else f"약 {gdp / 1e9:.1f}십억 USD"
    except Exception as e:
        log.warning(f"[{code}] GDP 지표 실패 → 캐시 사용: {e}")
        profile["gdp"] = cached_profile.get("gdp", "-")

    profile.update(get_inflation_unemployment(code, cached_profile))

    if code in STATUTORY_MIN_WAGE:
        profile["min_wage"] = STATUTORY_MIN_WAGE[code]["federal"]
        profile["min_wage_note"] = STATUTORY_MIN_WAGE[code]["note"]

    return profile


# ---------------------------------------------------------------------------
# 3. 환율 (현지통화 / KRW) — 일별 종가 기반 수집 + 자체 월별 리샘플
#    (yfinance의 interval="1mo" 대신 일봉을 받아 pandas로 직접 월별 종가를 뽑아
#     일부 통화에서 월봉이 비어 차트가 안 보이던 문제를 근본적으로 해결한다)
# ---------------------------------------------------------------------------
MIN_RELIABLE_TRADING_DAYS = 30  # 이보다 적으면 직접 페어를 신뢰하지 않고 교차 환산으로 전환


def _yf_daily_close(ticker: str, days: int = 400) -> pd.Series:
    import yfinance as yf
    hist = yf.Ticker(ticker).history(period=f"{days}d", interval="1d")
    if hist.empty:
        return pd.Series(dtype="float64")
    return hist["Close"].dropna()


def _monthly_from_daily(daily: pd.Series, months: int = 12):
    """일별 종가 시리즈 → (연,월) 그룹의 마지막 종가로 월별 시리즈 생성, 최근 months개월 반환."""
    if daily.empty:
        return [], []
    grouped = daily.groupby(daily.index.to_period("M")).last().tail(months)
    labels = [MONTH_KR[p.month - 1] for p in grouped.index]
    values = [round(float(v), 4) for v in grouped]
    return labels, values


def _direct_pair_series(currency: str) -> pd.Series:
    return _yf_daily_close(f"{currency}KRW=X")


def _cross_pair_series(currency: str) -> pd.Series:
    """직접 페어가 부실한 통화는 USD 경유 교차 환산: CUR/KRW = CUR/USD(=USD당 현지통화 아님, USD/1CUR 값) × USD/KRW."""
    cur_usd = _yf_daily_close(f"{currency}USD=X")
    usd_krw = _yf_daily_close("USDKRW=X")
    if cur_usd.empty or usd_krw.empty:
        return pd.Series(dtype="float64")
    combined = pd.DataFrame({"cur_usd": cur_usd, "usd_krw": usd_krw}).sort_index().ffill().dropna()
    if combined.empty:
        return pd.Series(dtype="float64")
    return combined["cur_usd"] * combined["usd_krw"]


def get_exchange_rate(country: dict, cached: dict) -> dict:
    currency = country["currency"]
    cached_fx = (cached or {}).get("exchange_rate", {})
    unit_base = SMALL_UNIT_CURRENCIES.get(currency, 1)

    if currency == "KRW":
        return {"is_base": True, "unit_base": 1, "current_rate": 1.0, "change_pct": 0.0,
                "history_labels": [], "history_values": [], "source": "기준통화"}

    try:
        daily = _direct_pair_series(currency)
        source = "Yahoo Finance"
        if len(daily) < MIN_RELIABLE_TRADING_DAYS:
            log.info(f"[{country['code']}] 직접 페어 데이터 부실({len(daily)}일) → 교차 환산으로 전환")
            cross = _cross_pair_series(currency)
            if len(cross) >= len(daily):  # 교차 환산이 더 신뢰할 만하면 채택
                daily, source = cross, "Yahoo Finance (USD 교차 환산)"

        if daily.empty:
            raise ValueError("일별 환율 데이터 없음(직접/교차 모두 실패)")

        current_rate_raw = float(daily.iloc[-1])
        change_pct = (
            round((current_rate_raw - float(daily.iloc[-2])) / float(daily.iloc[-2]) * 100, 2)
            if len(daily) >= 2 else 0.0
        )
        history_labels, history_values_raw = _monthly_from_daily(daily, months=12)
        if not history_labels:
            raise ValueError("월별 히스토리 생성 실패")

        return {
            "is_base": False,
            "unit_base": unit_base,
            "current_rate": round(current_rate_raw * unit_base, 2),
            "change_pct": change_pct,  # 비율이므로 단위 환산의 영향을 받지 않음
            "history_labels": history_labels,
            "history_values": [round(v * unit_base, 2) for v in history_values_raw],
            "source": source,
        }
    except Exception as e:
        log.warning(f"[{country['code']}] 환율 수집 실패 → 캐시/기본값 사용: {e}")
        if cached_fx:
            return cached_fx
        return {"is_base": False, "unit_base": unit_base, "current_rate": 0.0, "change_pct": 0.0,
                "history_labels": [], "history_values": [], "source": "수집 실패"}


# ---------------------------------------------------------------------------
# 4. 자동 번역 (영문 기사 대체 수집 시 사용)
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1024)
def _translate_to_ko(text: str) -> str:
    if not text:
        return text
    try:
        from deep_translator import GoogleTranslator
        return GoogleTranslator(source="auto", target="ko").translate(text)
    except Exception as e:
        log.warning(f"번역 실패, 원문 유지: {e}")
        return text


# ---------------------------------------------------------------------------
# 5. 뉴스 공통 유틸 — Google News RSS (한국어 1순위 / 영문 2순위)
# ---------------------------------------------------------------------------
def _google_news_url_ko(query: str) -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"


def _google_news_url_en(query: str) -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"


def _parse_entry(entry) -> dict:
    source = ""
    if hasattr(entry, "source"):
        source = getattr(entry.source, "title", "") or ""
    elif " - " in entry.title:
        source = entry.title.split(" - ")[-1]
    summary = _strip_html(getattr(entry, "summary", ""))
    return {
        "title": entry.title.split(" - ")[0] if " - " in entry.title else entry.title,
        "source": source or "Google News",
        "link": entry.link,
        "summary": _shorten(summary, 70) if summary else "",
    }


def _fetch_feed(url: str, limit: int) -> list:
    feed = feedparser.parse(url)
    return [_parse_entry(e) for e in feed.entries[:limit]]


def get_localized_items(query_kr: str, query_en: str, limit: int, when_filter: str) -> list:
    """1순위 한국어 검색 → 부족한 슬롯만 2순위 영문 검색 + 자동 번역으로 채운다."""
    results = []

    try:
        kr_items = _fetch_feed(_google_news_url_ko(f"{query_kr} {when_filter}"), limit=limit)
        for it in kr_items:
            it["lang"] = "ko"
            it["translated"] = False
        results.extend(kr_items)
    except Exception as e:
        log.warning(f"[KO 검색] 실패 ({query_kr}): {e}")

    remaining = limit - len(results)
    if remaining > 0:
        try:
            en_items = _fetch_feed(_google_news_url_en(f"{query_en} {when_filter}"), limit=remaining)
            for it in en_items:
                original_title = it["title"]
                original_summary = it["summary"]
                it["title"] = _translate_to_ko(original_title)
                it["summary"] = _translate_to_ko(original_summary) if original_summary else ""
                it["lang"] = "en"
                it["translated"] = True
                it["original_title"] = original_title
            results.extend(en_items)
        except Exception as e:
            log.warning(f"[EN 대체 검색] 실패 ({query_en}): {e}")

    return results


def get_headlines(country: dict, cached: dict) -> list:
    try:
        items = get_localized_items(
            query_kr=f"{country['name_kr']} 뉴스",
            query_en=country["name_en"],
            limit=3,
            when_filter="when:3d",
        )
        if not items:
            raise ValueError("빈 결과")
        return items
    except Exception as e:
        log.warning(f"[{country['code']}] 현지 뉴스 수집 실패 → 캐시 사용: {e}")
        return (cached or {}).get("headlines", [])


# ---------------------------------------------------------------------------
# 6. 비즈니스/HR 동향 — AUTO MARKET / HR & LABOR / ECONOMY / MANAGEMENT
# ---------------------------------------------------------------------------
INDUSTRY_TOPICS = [
    {
        "category": "AUTO MARKET", "tag": "완성차·타이어 시장", "tag_class": "bg-rose-100 text-rose-700",
        "query_en_tpl": "{name_en} auto OEM tire market EV demand",
        "query_kr_tpl": "{name_kr} 완성차 타이어 시장 전기차 수요",
    },
    {
        "category": "HR & LABOR", "tag": "노동법·인력", "tag_class": "bg-indigo-100 text-indigo-700",
        "query_en_tpl": "{name_en} labor law manufacturing wages hiring",
        "query_kr_tpl": "{name_kr} 노동법 제조업 임금 채용",
    },
    {
        "category": "ECONOMY", "tag": "경기·금리", "tag_class": "bg-emerald-100 text-emerald-700",
        "query_en_tpl": "{name_en} economy interest rate manufacturing PMI outlook",
        "query_kr_tpl": "{name_kr} 경제 기준금리 제조업 PMI 전망",
    },
    {
        "category": "MANAGEMENT", "tag": "관세·공급망", "tag_class": "bg-amber-100 text-amber-700",
        "query_en_tpl": "{name_en} tariff trade policy supply chain logistics cost",
        "query_kr_tpl": "{name_kr} 관세 통상 정책 공급망 물류비",
    },
]


def get_industry_trends(country: dict, cached: dict) -> list:
    cached_by_category = {t.get("category"): t for t in (cached or {}).get("hr_trends", [])}
    trends = []

    for spec in INDUSTRY_TOPICS:
        query_kr = spec["query_kr_tpl"].format(name_kr=country["name_kr"])
        query_en = spec["query_en_tpl"].format(name_en=country["name_en"])
        try:
            item = get_localized_items(query_kr, query_en, limit=1, when_filter="when:14d")[0]
            trends.append({
                "category": spec["category"],
                "tag": spec["tag"],
                "tag_class": spec["tag_class"],
                "title": _shorten(item["title"], 46),
                "desc": item["summary"] or _shorten(item["title"], 70),
                "source": item["source"],
                "link": item["link"],
                "lang": item.get("lang", "ko"),
                "translated": item.get("translated", False),
                "original_title": item.get("original_title", ""),
            })
        except Exception as e:
            log.warning(f"[{country['code']}] '{spec['category']}' 산업 동향 실패: {e}")
            cached_item = cached_by_category.get(spec["category"])
            if cached_item:
                trends.append(cached_item)

    return trends


# ---------------------------------------------------------------------------
# 7. 국가 단위 수집 오케스트레이션
# ---------------------------------------------------------------------------
def collect_country(country: dict, cache_by_code: dict) -> dict:
    code = country["code"]
    cached = cache_by_code.get(code, {})
    log.info(f"[{code}] 수집 시작 — {country['name_kr']}")

    result = dict(country)
    result["profile"] = build_profile(country, cached)
    result["exchange_rate"] = get_exchange_rate(country, cached)
    result["headlines"] = get_headlines(country, cached)
    result["hr_trends"] = get_industry_trends(country, cached)

    log.info(f"[{code}] 수집 완료")
    return result


def main() -> dict:
    cache_by_code = load_cache()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(collect_country, c, cache_by_code): c["code"] for c in COUNTRIES}
        collected = {}
        for future in as_completed(futures):
            code = futures[future]
            try:
                collected[code] = future.result()
            except Exception as e:
                log.error(f"[{code}] 국가 단위 수집 실패, 캐시로 대체: {e}")
                fallback_meta = next(c for c in COUNTRIES if c["code"] == code)
                collected[code] = cache_by_code.get(code) or {
                    **fallback_meta,
                    "profile": {}, "exchange_rate": {"is_base": False, "unit_base": 1},
                    "headlines": [], "hr_trends": [],
                }
        # 원본 COUNTRIES 순서를 유지해 UI 정렬을 안정적으로 유지
        countries_result = [collected[c["code"]] for c in COUNTRIES]

    now_kst = datetime.now(KST)
    data = {
        "generated_at": now_kst.isoformat(),
        "generated_at_display": now_kst.strftime("%Y-%m-%d %H:%M KST"),
        "regions": REGIONS,
        "countries": countries_result,
    }

    save_cache(data)
    save_archive(data)
    log.info(f"전체 {len(countries_result)}개국 수집 완료 → {CACHE_PATH} / {ARCHIVE_DIR}")
    return data


if __name__ == "__main__":
    main()
