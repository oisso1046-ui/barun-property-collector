from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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
    {"name": "사송우미린포레스트", "number": "147898"},
    {"name": "사송더샵데시앙1단지", "number": "132577"},
    {"name": "사송더샵데시앙3단지", "number": "191257"},
    {"name": "사송더샵데시앙4단지", "number": "125854"},
    {"name": "사송더샵데시앙5단지", "number": "103526"},
    {"name": "사송더샵데시앙6단지", "number": "125853"},
    {"name": "사송더샵데시앙7단지", "number": "125855"},
    {"name": "사송트루엘", "number": "129865"},
    {"name": "사송에비뉴원", "number": "151001"},
    {"name": "사송제일풍경채", "number": "144883"},
]

REGION_BOXES = [
        {
                    "name": "금정구(사송)",
                    "box": {
                                    "left": 129.05031982887942,
                                    "right": 129.12130097112458,
                                    "top": 35.32954512979022,
                                    "bottom": 35.28137470013591,
                    },
        },
        {
                    "name": "부산강서구",
                    "box": {
                                    "left": 128.82,
                                    "right": 129.02,
                                    "top": 35.25,
                                    "bottom": 35.05,
                    },
        },
        {
                    "name": "김해진영",
                    "box": {
                                    "left": 128.70,
                                    "right": 128.80,
                                    "top": 35.35,
                                    "bottom": 35.28,
                    },
        },
        {
                    "name": "창원",
                    "box": {
                                    "left": 128.55,
                                    "right": 128.75,
                                    "top": 35.30,
                                    "bottom": 35.15,
                    },
        },
]
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
            http2=False,
            follow_redirects=True,
            timeout=httpx.Timeout(connect=20.0, read=45.0, write=20.0, pool=20.0),
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
        """GitHub에서는 느린 지도 초기 접속을 건너뜁니다."""
        self.client.cookies.clear()

        if os.getenv("GITHUB_ACTIONS", "").lower() == "true":
            log.info("GitHub Actions 환경: 초기 지도 접속 생략")
            return

        try:
            response = self.client.get(MAP_URL, timeout=15.0)
            response.raise_for_status()
            log.info("세션 초기화 완료")
        except httpx.HTTPError as exc:
            log.warning("초기 접속 실패, API를 바로 호출합니다: %s", exc)


    def request_json(
        self,
        method: str,
        url: str,
        *,
        payload: dict[str, Any] | None = None,
        retries: int = 5,
    ) -> dict[str, Any]:
        last_error: Exception | None = None

        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                response = self.client.request(
                    method,
                    url,
                    json=payload,
                    headers={"Connection": "close"},
                )

                if response.status_code == 429:
                    wait = min(60, 10 * (attempt + 1))
                    log.warning("HTTP 429, %s초 대기", wait)
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except (httpx.HTTPError, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt >= retries:
                    break

                wait = min(30, 3 * (attempt + 1))
                log.warning(
                    "API 요청 재시도 %s/%s: %s",
                    attempt + 1,
                    retries,
                    exc,
                )
                time.sleep(wait)

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

    def fetch_region(self, page_size: int, max_pages: int, bounding_box: dict[str, float]) -> list[dict[str, Any]]:
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
                "boundingBox": bounding_box,
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


def find_service_account_file() -> Path:
    """PC 실행 시 현재 폴더에서 Google 서비스계정 JSON 파일을 찾습니다."""
    base_dir = Path(__file__).resolve().parent
    for path in base_dir.glob("*.json"):
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("type") == "service_account" and data.get("client_email"):
                return path
        except (OSError, json.JSONDecodeError):
            continue
    raise FileNotFoundError(
        "서비스계정 JSON 파일을 찾지 못했습니다. "
        "PC에서는 main.py와 같은 폴더에 서비스계정 .json 파일을 넣어주세요."
    )


def get_spreadsheet(settings: Settings) -> gspread.Spreadsheet:
    """GitHub에서는 Secret을, PC에서는 같은 폴더의 JSON 파일을 사용합니다."""
    secret_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    if secret_json:
        try:
            info = json.loads(secret_json)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "GitHub Secret GOOGLE_SERVICE_ACCOUNT_JSON 형식이 올바르지 않습니다."
            ) from exc
        log.info("GitHub Secret 서비스계정 사용")
    else:
        service_account_path = find_service_account_file()
        info = json.loads(service_account_path.read_text(encoding="utf-8"))
        log.info("PC 서비스계정 파일 사용: %s", service_account_path.name)

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
    """기존 행 수정은 한 번의 batch_update로 처리해 Google Sheets 쿼터를 절약합니다."""
    existing_values = ws.get_all_values()
    existing_map: dict[str, int] = {}

    for idx, row in enumerate(existing_values[1:], start=2):
        if len(row) > id_index and row[id_index]:
            existing_map[str(row[id_index])] = idx

    new_rows: list[list[Any]] = []
    update_requests: list[dict[str, Any]] = []

    for row in rows:
        article_id = str(row[id_index]).strip()
        if not article_id:
            continue

        if article_id in existing_map:
            target_row = existing_map[article_id]
            update_requests.append({
                "range": f"A{target_row}:{column_letter(len(headers))}{target_row}",
                "values": [row],
            })
        else:
            new_rows.append(row)

    if update_requests:
        ws.batch_update(update_requests, value_input_option="RAW")

    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW")

    return len(new_rows), len(update_requests)


def column_letter(number: int) -> str:
    """1부터 시작하는 열 번호를 A, B, ... 형식으로 변환합니다."""
    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result

def normalize_apartment(
    item: dict[str, Any],
    complex_name: str,
    complex_number: str,
) -> list[Any]:
    price_value = deep_first(
        item,
        ["priceText", "dealPrice", "price", "priceInfo", "warrantPrice", "rentPrice"],
    )
    return [
        now_text(),
        clean_value(item, ["articleNumber", "articleNo", "articleId", "id"]),
        complex_name,
        complex_number,
        clean_value(item, ["tradeTypeName", "tradeType", "dealTypeName"]),
        format_price(price_value),
        clean_value(item, ["dongName", "buildingName", "dong", "articleBuildingName"]),
        clean_value(item, ["floorInfo", "floor", "articleFloor"]),
        clean_value(item, ["supplySpace", "supplyArea", "area1", "pyeongName"]),
        clean_value(item, ["exclusiveSpace", "exclusiveArea", "area2"]),
        clean_value(item, ["direction", "directionName"]),
        clean_value(item, ["articleName", "articleTitle", "description", "featureDescription"]),
        clean_value(item, ["realtorName", "brokerageName", "cpName"]),
        clean_value(item, ["verificationDate", "confirmDate", "articleConfirmYmd"]),
        "현재",
        clean_value(item, ["realEstateTypeName", "realEstateTypeCode", "realEstateType"]),
        truncate(json.dumps(item, ensure_ascii=False), 45000),
    ]

def normalize_region(item: dict[str, Any]) -> list[Any]:
    price_value = deep_first(
        item,
        ["priceText", "dealPrice", "price", "priceInfo", "warrantPrice", "rentPrice"],
    )
    return [
        now_text(),
        clean_value(item, ["articleNumber", "articleNo", "articleId", "id"]),
        clean_value(item, ["realEstateTypeName", "realEstateType", "realEstateTypeCode"]),
        clean_value(item, ["tradeTypeName", "tradeType", "dealTypeName"]),
        format_price(price_value),
        clean_value(item, ["address", "roadAddress", "location", "articleAddress", "detailAddress"]),
        clean_value(item, ["floorInfo", "floor", "articleFloor"]),
        clean_value(item, ["exclusiveSpace", "supplySpace", "area", "landArea", "space"]),
        clean_value(item, ["articleName", "articleTitle", "description", "featureDescription"]),
        clean_value(item, ["realtorName", "brokerageName", "cpName"]),
        clean_value(item, ["verificationDate", "confirmDate", "articleConfirmYmd"]),
        "현재",
        clean_value(item, ["realEstateTypeCode", "realEstateTypeName", "realEstateType"]),
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
        sheet_id=os.getenv("GOOGLE_SHEET_ID", "1Ax_z5OcACNd2Eb2Th19MD0LVgDR6O2dMU68hJmTPFJE"),
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

        region_items = [item for region in REGION_BOXES for item in client.fetch_region(settings.page_size, settings.max_pages, region["box"])]
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
