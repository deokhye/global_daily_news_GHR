"""
build_site.py
--------------
data/countries_data.json (오늘자 데이터) 을 template/template.html 에 임베드하여
docs/index.html 을 생성한다.

v5 변경점: docs/archive/index.json (collector.py가 기록한 아카이브 날짜 목록)을 함께 읽어
날짜 피커(<input type="date">)의 min/max 범위를 템플릿에 전달한다. 아카이브가 아직
없는 최초 실행 시에는 오늘 날짜만 선택 가능한 상태로 안전하게 폴백한다.

실행:
    python src/collector.py    # 39개국 수집 -> data/countries_data.json + docs/archive/*.json
    python src/build_site.py   # 템플릿에 데이터 임베드 -> docs/index.html
"""

import os
import json
import logging

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("build_site")

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_PATH = os.path.join(BASE_DIR, "data", "countries_data.json")
ARCHIVE_INDEX_PATH = os.path.join(BASE_DIR, "docs", "archive", "index.json")
TEMPLATE_DIR = os.path.join(BASE_DIR, "template")
TEMPLATE_NAME = "template.html"
OUTPUT_PATH = os.path.join(BASE_DIR, "docs", "index.html")

REQUIRED_PROFILE_FIELDS = ["capital", "population", "gdp", "inflation", "unemployment"]


def load_data() -> dict:
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_archive_range(today_str: str) -> tuple:
    """아카이브 인덱스에서 (최소 날짜, 최대 날짜)를 읽는다. 없으면 오늘 하루만 유효 범위로 폴백."""
    if os.path.exists(ARCHIVE_INDEX_PATH):
        try:
            with open(ARCHIVE_INDEX_PATH, "r", encoding="utf-8") as f:
                idx = json.load(f)
            min_date = idx.get("min_date") or today_str
            max_date = idx.get("latest") or today_str
            return min_date, max_date
        except Exception as e:
            log.warning(f"아카이브 인덱스 로드 실패 → 오늘 날짜로 폴백: {e}")
    else:
        log.warning("docs/archive/index.json 이 아직 없습니다 (최초 실행) → 오늘 날짜로 폴백")
    return today_str, today_str


def _validate(data: dict) -> None:
    countries = data.get("countries", [])
    if not countries:
        log.warning("countries 목록이 비어 있습니다 — collector.py를 먼저 실행했는지 확인하세요.")
        return
    for c in countries:
        profile = c.get("profile", {})
        missing = [f for f in REQUIRED_PROFILE_FIELDS if not profile.get(f)]
        if missing:
            log.warning(f"[{c.get('code')}] profile 필드 누락: {missing} — 화면에는 '-'로 표시됩니다.")
        if "exchange_rate" not in c:
            log.warning(f"[{c.get('code')}] exchange_rate 필드 자체가 없습니다.")
        if "headlines" not in c or "hr_trends" not in c:
            log.warning(f"[{c.get('code')}] headlines/hr_trends 필드 누락.")


def build(data: dict) -> None:
    _validate(data)

    today_str = data["generated_at"][:10]
    archive_min, archive_max = load_archive_range(today_str)

    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template(TEMPLATE_NAME)

    # 클라이언트(JS)가 그대로 소비할 하나의 JSON 블록.
    # 1) </script> 시퀀스가 문자열 안에 우연히 포함돼도 HTML 파싱이 깨지지 않도록 이스케이프
    # 2) Markup()으로 감싸 Jinja2 autoescape가 다시 HTML 이스케이프하지 않도록 표시
    #    (템플릿에서도 {{ dashboard_json|safe }} 로 이중 방어)
    raw_json = json.dumps(
        {"regions": data["regions"], "countries": data["countries"]},
        ensure_ascii=False,
    ).replace("</", "<\\/")
    dashboard_json = Markup(raw_json)

    ctx = {
        "generated_at_display": data["generated_at_display"],
        "generated_at_date": today_str,       # 날짜 피커 초기값(오늘)
        "archive_min_date": archive_min,       # 날짜 피커 min
        "archive_max_date": archive_max,       # 날짜 피커 max
        "dashboard_json": dashboard_json,
    }

    html = template.render(**ctx)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    log.info(f"생성 완료 → {OUTPUT_PATH} ({len(data['countries'])}개국, 아카이브 범위 {archive_min}~{archive_max})")


def main():
    build(load_data())


if __name__ == "__main__":
    main()
