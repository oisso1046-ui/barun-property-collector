from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import gspread
import httpx
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("barun-collector")

BASE_URL = "https://fin.land.naver.com"
MAP_URL = f"{BASE_URL}/map"
APT_URL = f"{BASE_URL}/front-api/v1/complex/article/list"
REGION_URL = f"{BASE_URL}/front-api/v1/article/boundedArticles"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)

DEFAULT_COMPLEXES = [
    {"name": "더샵데시앙 1단지", "number": "132577"},
    {"name": "더샵데시앙 3차", "number": "141426"},
    {"name": "더샵데시앙 4단지", "number": "125854"},
    {"name": "사송트루엘", "number": "129865"},
    {"name": "사송에비뉴원", "number": "151001"},
    {"name": "사송제일풍경채", "number": "144883"},
]

BOUNDING_BOX = {
    "left": 129.05031982887942,
    "right": 129.12130097112458,
    "top": 35.32954512979022,
    "bottom": 35.28137470013591,
}

TRADE_TYPES = ["A1", "B2", "B1", "B3"]
REGION_TYPES = ["D02", "D01", "D03", "D04", "E01", "Z00", "E03", "E02", "E04"]

APT_HEADERS = [
    "수집일시", "매물번호", "단지명", "단지번호", "거래유형", "가격", "동", "층",
    "공급면적", "전용면적", "방향", "매물설명", "중개사", "확인일", "상태",
    "원본유형", "원본JSON",
]

REGION_HEADERS = [
    "수집일시", "매물번호", "매물종류", "거래유형", "가격", "주소/위치", "층",
    "면적", "매물설명", "중개사", "확인일", "상태", "원본유형", "원본JSON",
]

LOG_HEADERS = ["실행일시", "작업", "결과", "건수", "메시지"]


@dataclass
class Settings:
    sheet_id: str
    request_delay: float = 0.8
    max_pages: int = 20
    page_size: int = 30


class LandClient:
    def __init__(self, delay: float = 0.8) -> None:
        self.delay = delay
        self.client = httpx.Client(
            http2=True,
            follow_redirects=True,
            timeout=httpx.Timeout(25.0),
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Content-Type": "application/json",
                "Origin": BASE_URL,
                "Referer": f"{BASE_URL}/",
            },
        )

    def close(self) -> None:
        self.client.close()
    def bootstrap(self) -> None:
    """네이버 초기 접속을 시도하되, 시간 초과 시 API 호출을 계속 진행."""
    self.client.cookies.clear()

    for attempt in range(3):
        try:
            response = self.client.get(
                MAP_URL,
                headers={
                    "Accept": (
                        "text/html,application/xhtml+xml,application/xml;q=0.9,"
                        "image/avif,image/webp,*/*;q=0.8"
                    ),
                    "Content-Type": "text/html",
                },
                timeout=60.0,
            )
            response.raise_for_status()
            log.info(
                "세션 쿠키 초기화: %s",
                sorted(self.client.cookies.keys()),
            )
            return

        except httpx.HTTPError as exc:
            log.warning(
                "초기 접속 실패 %s/3: %s",
                attempt + 1,
                exc,
            )
            time.sleep(3 * (attempt + 1))

    log.warning("초기 접속을 건너뛰고 API 요청을 계속합니다.")
  

    def request_json(
        self,
        method: str,
        url: str,
        *,
        payload: dict[str, Any] | None = None,
        retries: int = 2,
    ) -> dict[str, Any]:
        last_error: Exception | None = None

        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                response = self.client.request(method, url, json=payload)

                if response.status_code in {401, 403}:
                    if attempt >= retries:
                        raise RuntimeError(f"접근 거부 HTTP {response.status_code}")
                    log.warning("세션 재초기화 후 재시도: HTTP %s", response.status_code)
                    self.bootstrap()
                    continue

                if response.status_code == 429:
                    wait = min(30, 5 * (attempt + 1))
                    log.warning("요청 제한 HTTP 429, %s초 대기", wait)
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except (httpx.HTTPError, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt >= retries:
                    break
                time.sleep(2 ** attempt)

        raise RuntimeError(f"API 요청 실패: {last_error}")

    def fetch_complex(self, complex_number: str, page_size: int, max_pages: int) -> list[dict[str, Any]]:
        all_items: list[dict[str, Any]] = []
        seen: set[str] = set()
        last_info: list[Any] = []

        for _ in range(max_pages):
            payload = {
                "size": page_size,
                "complexNumber": str(complex_number),
                "tradeTypes": [],
                "pyeongTypes": [],
                "dongNumbers": [],
                "userChannelType": "PC",
                "articleSortType": "RANKING_DESC",
                "lastInfo": last_info,
            }
            data = self.request_json("POST", APT_URL, payload=payload)
            items = find_article_array(data)
            if not items:
                break

            added = 0
            for item in items:
                article_id = str(first(item, ["articleNumber", "articleNo", "articleId", "id"]))
                key = article_id or json.dumps(item, ensure_ascii=False, sort_keys=True)
                if key not in seen:
                    seen.add(key)
                    all_items.append(item)
                    added += 1

            next_last = find_key(data, "lastInfo")
            if not isinstance(next_last, list) or not next_last or added == 0:
                break
            last_info = next_last

        return all_items

    def fetch_region(self, page_size: int, max_pages: int) -> list[dict[str, Any]]:
        all_items: list[dict[str, Any]] = []
        seen: set[str] = set()
        last_info: list[Any] = []

        for _ in range(max_pages):
            payload = {
                "filter": {
                    "tradeTypes": TRADE_TYPES,
                    "realEstateTypes": REGION_TYPES,
                    "roomCount": [],
                    "bathRoomCount": [],
                    "optionTypes": [],
                    "oneRoomShapeTypes": [],
                    "moveInTypes": [],
                    "filtersExclusiveSpace": False,
                    "floorTypes": [],
                    "directionTypes": [],
                    "hasArticlePhoto": False,
                    "isAuthorizedByOwner": False,
                    "parkingTypes": [],
                    "entranceTypes": [],
                    "hasArticle": False,
                },
                "boundingBox": BOUNDING_BOX,
                "precision": 13.463091604384706,
                "userChannelType": "PC",
                "articlePagingRequest": {
                    "size": page_size,
                    "articleSortType": "RANKING_DESC",
                    "lastInfo": last_info,
                },
            }
            data = self.request_json("POST", REGION_URL, payload=payload)
            items = find_article_array(data)
            if not items:
                break

            added = 0
            for item in items:
                article_id = str(first(item, ["articleNumber", "articleNo", "articleId", "id"]))
                key = article_id or json.dumps(item, ensure_ascii=False, sort_keys=True)
                if key not in seen:
                    seen.add(key)
                    all_items.append(item)
                    added += 1

            next_last = find_key(data, "lastInfo")
            if not isinstance(next_last, list) or not next_last or added == 0:
                break
            last_info = next_last

        return all_items


def get_spreadsheet(settings: Settings) -> gspread.Spreadsheet:
    raw = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(settings.sheet_id)


def ensure_worksheet(book: gspread.Spreadsheet, title: str, headers: list[str]) -> gspread.Worksheet:
    try:
        ws = book.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = book.add_worksheet(title=title, rows=5000, cols=max(20, len(headers)))
    if ws.row_values(1) != headers:
        ws.update("A1", [headers])
    return ws


def load_complexes(book: gspread.Spreadsheet) -> list[dict[str, str]]:
    try:
        ws = book.worksheet("단지번호관리")
        records = ws.get_all_records()
        complexes = []
        for row in records:
            if str(row.get("사용여부", "")).strip() != "사용":
                continue
            name = str(row.get("단지명", "")).strip()
            number = str(row.get("단지번호", "")).strip()
            if name and number:
                complexes.append({"name": name, "number": number})
        return complexes or DEFAULT_COMPLEXES
    except gspread.WorksheetNotFound:
        return DEFAULT_COMPLEXES


def upsert_rows(
    ws: gspread.Worksheet,
    headers: list[str],
    rows: list[list[Any]],
    id_index: int = 1,
) -> tuple[int, int]:
    existing_values = ws.get_all_values()
    existing_map: dict[str, int] = {}
    for idx, row in enumerate(existing_values[1:], start=2):
        if len(row) > id_index and row[id_index]:
            existing_map[row[id_index]] = idx

    new_rows: list[list[Any]] = []
    updated = 0
    for row in rows:
        article_id = str(row[id_index])
        if not article_id:
            continue
        if article_id in existing_map:
            target = existing_map[article_id]
            ws.update(f"A{target}", [row])
            updated += 1
        else:
            new_rows.append(row)

    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW")

    return len(new_rows), updated


def normalize_apartment(item: dict[str, Any], complex_name: str, complex_number: str) -> list[Any]:
    return [
        now_text(),
        text(first(item, ["articleNumber", "articleNo", "articleId", "id"])),
        complex_name,
        complex_number,
        text(first(item, ["tradeTypeName", "tradeType", "dealTypeName"])),
        text(first(item, ["price", "dealPrice", "priceText", "priceInfo"])),
        text(first(item, ["dongName", "buildingName", "dong", "articleBuildingName"])),
        text(first(item, ["floorInfo", "floor", "articleFloor"])),
        text(first(item, ["supplySpace", "supplyArea", "area1", "pyeongName"])),
        text(first(item, ["exclusiveSpace", "exclusiveArea", "area2"])),
        text(first(item, ["direction", "directionName"])),
        text(first(item, ["articleName", "articleTitle", "description", "featureDescription"])),
        text(first(item, ["realtorName", "brokerageName", "cpName"])),
        text(first(item, ["verificationDate", "confirmDate", "articleConfirmYmd"])),
        "현재",
        text(first(item, ["realEstateTypeName", "realEstateTypeCode", "realEstateType"])),
        truncate(json.dumps(item, ensure_ascii=False), 45000),
    ]


def normalize_region(item: dict[str, Any]) -> list[Any]:
    return [
        now_text(),
        text(first(item, ["articleNumber", "articleNo", "articleId", "id"])),
        text(first(item, ["realEstateTypeName", "realEstateType", "realEstateTypeCode"])),
        text(first(item, ["tradeTypeName", "tradeType", "dealTypeName"])),
        text(first(item, ["price", "dealPrice", "priceText", "priceInfo"])),
        text(first(item, ["address", "roadAddress", "location", "articleAddress", "detailAddress"])),
        text(first(item, ["floorInfo", "floor", "articleFloor"])),
        text(first(item, ["exclusiveSpace", "supplySpace", "area", "landArea", "space"])),
        text(first(item, ["articleName", "articleTitle", "description", "featureDescription"])),
        text(first(item, ["realtorName", "brokerageName", "cpName"])),
        text(first(item, ["verificationDate", "confirmDate", "articleConfirmYmd"])),
        "현재",
        text(first(item, ["realEstateTypeCode", "realEstateTypeName", "realEstateType"])),
        truncate(json.dumps(item, ensure_ascii=False), 45000),
    ]


def is_land(row: list[Any]) -> bool:
    label = str(row[2])
    raw_type = str(row[12])
    return bool(
        any(word in label for word in ["토지", "대지", "전", "답", "임야"])
        or raw_type == "E03"
    )


def find_article_array(value: Any) -> list[dict[str, Any]]:
    candidates: list[list[dict[str, Any]]] = []

    def visit(node: Any) -> None:
        if isinstance(node, list):
            if node and any(
                isinstance(item, dict)
                and any(key in item for key in [
                    "articleNumber", "articleNo", "articleId",
                    "tradeTypeName", "realEstateTypeName", "priceInfo"
                ])
                for item in node
            ):
                candidates.append(node)
            for child in node:
                visit(child)
        elif isinstance(node, dict):
            for child in node.values():
                visit(child)

    visit(value)
    candidates.sort(key=len, reverse=True)
    return candidates[0] if candidates else []


def find_key(value: Any, wanted: str) -> Any:
    if isinstance(value, dict):
        if wanted in value:
            return value[wanted]
        for child in value.values():
            result = find_key(child, wanted)
            if result is not None:
                return result
    elif isinstance(value, list):
        for child in value:
            result = find_key(child, wanted)
            if result is not None:
                return result
    return None


def first(obj: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = obj.get(key)
        if value not in (None, ""):
            return value
    return ""


def text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def truncate(value: str, max_len: int) -> str:
    return value[:max_len]


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def write_log(ws: gspread.Worksheet, job: str, result: str, count: int, message: str) -> None:
    ws.append_row([now_text(), job, result, count, message], value_input_option="RAW")


def run() -> None:
    settings = Settings(
        sheet_id=os.environ["GOOGLE_SHEET_ID"],
        request_delay=float(os.getenv("REQUEST_DELAY", "0.8")),
        max_pages=int(os.getenv("MAX_PAGES", "20")),
        page_size=int(os.getenv("PAGE_SIZE", "30")),
    )

    book = get_spreadsheet(settings)
    apt_ws = ensure_worksheet(book, "아파트매물DB", APT_HEADERS)
    commercial_ws = ensure_worksheet(book, "상가건물매물DB", REGION_HEADERS)
    land_ws = ensure_worksheet(book, "토지매물DB", REGION_HEADERS)
    log_ws = ensure_worksheet(book, "실행로그", LOG_HEADERS)

    client = LandClient(settings.request_delay)
    try:
        client.bootstrap()

        apt_total = 0
        for complex_info in load_complexes(book):
            items = client.fetch_complex(
                complex_info["number"],
                settings.page_size,
                settings.max_pages,
            )
            rows = [
                normalize_apartment(item, complex_info["name"], complex_info["number"])
                for item in items
            ]
            new_count, updated_count = upsert_rows(apt_ws, APT_HEADERS, rows)
            apt_total += len(rows)
            write_log(
                log_ws,
                f"아파트:{complex_info['name']}",
                "성공",
                len(rows),
                f"신규 {new_count} / 갱신 {updated_count}",
            )

        region_items = client.fetch_region(settings.page_size, settings.max_pages)
        region_rows = [normalize_region(item) for item in region_items]
        land_rows = [row for row in region_rows if is_land(row)]
        commercial_rows = [row for row in region_rows if not is_land(row)]

        c_new, c_up = upsert_rows(commercial_ws, REGION_HEADERS, commercial_rows)
        l_new, l_up = upsert_rows(land_ws, REGION_HEADERS, land_rows)

        write_log(
            log_ws,
            "지역매물",
            "성공",
            len(region_rows),
            f"상가·건물 신규 {c_new}/갱신 {c_up}, 토지 신규 {l_new}/갱신 {l_up}",
        )
        write_log(log_ws, "전체수집", "성공", apt_total + len(region_rows), "완료")

    except Exception as exc:
        write_log(log_ws, "전체수집", "실패", 0, str(exc))
        raise
    finally:
        client.close()


if __name__ == "__main__":
    run()
