#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag


TOP250_URL = "https://movie.douban.com/top250"
DEFAULT_TOP250_OUTPUT = "top250_movies.json"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://movie.douban.com/",
    "Connection": "close",
}

FIELD_MAP = {
    "导演": "directors",
    "编剧": "writers",
    "主演": "actors",
    "类型": "genres",
    "制片国家/地区": "countries",
    "语言": "languages",
    "上映日期": "release_dates",
    "片长": "runtimes",
    "又名": "aka",
    "官方网站": "official_site",
    "IMDb": "imdb",
}

LIST_FIELDS = {
    "directors",
    "writers",
    "actors",
    "genres",
    "countries",
    "languages",
    "release_dates",
    "runtimes",
    "aka",
}


def normalize_text(text: str | None) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def html_fragment_to_text(fragment: str) -> str:
    fragment = re.sub(r"(?is)<script.*?>.*?</script>", "", fragment)
    fragment = re.sub(r"(?is)<style.*?>.*?</style>", "", fragment)
    fragment = re.sub(r"(?i)<br\s*/?>", "\n", fragment)
    fragment = re.sub(r"(?i)</p\s*>", "\n", fragment)
    fragment = re.sub(r"(?i)</div\s*>", "\n", fragment)
    fragment = re.sub(r"<[^>]+>", "", fragment)
    lines = [normalize_text(line) for line in fragment.splitlines()]
    return " ".join(line for line in lines if line)


def split_list_value(value: str) -> list[str]:
    return [item.strip() for item in re.split(r"\s*/\s*", value) if item.strip()]


def parse_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    if not digits:
        return None
    return int(digits)


def subject_to_url(subject: str) -> tuple[str, str]:
    subject = subject.strip()
    if re.fullmatch(r"\d+", subject):
        subject_id = subject
    else:
        match = re.search(r"https?://movie\.douban\.com/subject/(\d+)", subject)
        if not match:
            raise ValueError("请输入豆瓣电影详情页 URL 或纯数字的 subject ID。")
        subject_id = match.group(1)

    return subject_id, f"https://movie.douban.com/subject/{subject_id}/"


def set_response_encoding(response: requests.Response) -> None:
    response.encoding = response.apparent_encoding or response.encoding or "utf-8"


def is_security_page(url: str, html: str) -> bool:
    return "sec.douban.com" in url or ('id="tok"' in html and 'id="cha"' in html)


def extract_difficulty(html: str) -> int:
    for pattern in (r"difficulty\s*=\s*(\d+)", r"process\(cha,\s*(\d+)\)"):
        match = re.search(pattern, html)
        if match:
            return int(match.group(1))
    return 4


def solve_pow(challenge: str, difficulty: int) -> int:
    prefix = "0" * difficulty
    nonce = 0

    while True:
        nonce += 1
        digest = hashlib.sha512(f"{challenge}{nonce}".encode("utf-8")).hexdigest()
        if digest.startswith(prefix):
            return nonce


def find_movie_jsonld(data: Any) -> dict[str, Any] | None:
    if isinstance(data, dict):
        value_type = data.get("@type")
        if value_type == "Movie" or (isinstance(value_type, list) and "Movie" in value_type):
            return data
        for value in data.values():
            found = find_movie_jsonld(value)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = find_movie_jsonld(item)
            if found:
                return found
    return None


def extract_movie_jsonld(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.select('script[type="application/ld+json"]'):
        raw_json = script.string or script.get_text(strip=True)
        if not raw_json:
            continue
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        movie = find_movie_jsonld(data)
        if movie:
            return movie
    return {}


def jsonld_people(value: Any) -> list[str]:
    items = value if isinstance(value, list) else ([value] if value else [])
    people: list[str] = []

    for item in items:
        if isinstance(item, dict):
            name = normalize_text(item.get("name"))
        else:
            name = normalize_text(str(item))
        if name:
            people.append(name)

    return people


def collect_until_br(label: Tag) -> str:
    parts: list[str] = []

    for sibling in label.next_siblings:
        if isinstance(sibling, Tag) and sibling.name == "br":
            break
        if isinstance(sibling, NavigableString):
            text = normalize_text(str(sibling))
        elif isinstance(sibling, Tag):
            text = normalize_text(sibling.get_text(" ", strip=True))
        else:
            text = ""
        if text:
            parts.append(text)

    value = normalize_text(" ".join(parts))
    return re.sub(r"^[：:]\s*", "", value)


def parse_info_block(soup: BeautifulSoup) -> dict[str, Any]:
    info = soup.select_one("#info")
    if info is None:
        return {}

    result: dict[str, Any] = {}
    for label in info.select("span.pl"):
        label_text = normalize_text(label.get_text(strip=True)).rstrip(":：")
        key = FIELD_MAP.get(label_text)
        if not key:
            continue

        value = collect_until_br(label)
        if not value:
            continue

        if key in LIST_FIELDS:
            result[key] = split_list_value(value)
        else:
            result[key] = value

    return result


def extract_summary(html: str, movie_json: dict[str, Any]) -> str:
    hidden_match = re.search(
        r'<span class="all hidden">\s*(.*?)\s*</span>\s*<span class="pl">',
        html,
        re.DOTALL,
    )
    if hidden_match:
        summary = html_fragment_to_text(hidden_match.group(1))
        if summary:
            return summary

    candidates = [
        html_fragment_to_text(match)
        for match in re.findall(
            r'<span[^>]*property=["\']v:summary["\'][^>]*>(.*?)</span>',
            html,
            re.DOTALL,
        )
    ]
    candidates = [candidate for candidate in candidates if candidate]
    if candidates:
        return max(candidates, key=len)

    return normalize_text(movie_json.get("description"))


def extract_tags(soup: BeautifulSoup) -> list[str]:
    return [
        tag
        for tag in (
            normalize_text(element.get_text(strip=True))
            for element in soup.select(".tags-body a")
        )
        if tag
    ]


def extract_top250_entries(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict[str, Any]] = []

    for item in soup.select("ol.grid_view > li"):
        link = item.select_one(".hd a")
        if link is None:
            continue

        href = link.get("href", "")
        match = re.search(r"/subject/(\d+)/", href)
        if not match:
            continue

        rank_element = item.select_one("em")
        title_elements = item.select(".hd .title")
        quote_element = item.select_one(".inq")

        other_titles = [
            normalize_text(element.get_text(strip=True)).lstrip("/").strip()
            for element in title_elements[1:]
        ]

        entries.append(
            {
                "subject_id": match.group(1),
                "url": href,
                "top250_rank": parse_int(rank_element.get_text(strip=True) if rank_element else None),
                "top250_list_title": normalize_text(title_elements[0].get_text(strip=True) if title_elements else None),
                "top250_other_titles": [title for title in other_titles if title],
                "top250_quote": normalize_text(quote_element.get_text(strip=True) if quote_element else None) or None,
            }
        )

    return entries


def parse_movie_detail(subject_id: str, url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    movie_json = extract_movie_jsonld(soup)
    info_data = parse_info_block(soup)

    title_element = soup.select_one('span[property="v:itemreviewed"]')
    title = normalize_text(title_element.get_text(" ", strip=True) if title_element else None)
    if not title:
        title = normalize_text(movie_json.get("name"))

    year = None
    year_element = soup.select_one("span.year")
    if year_element is not None:
        year_match = re.search(r"(\d{4})", year_element.get_text())
        year = year_match.group(1) if year_match else None

    rating_json = movie_json.get("aggregateRating", {}) if isinstance(movie_json, dict) else {}
    rating_value_element = soup.select_one('strong[property="v:average"]')
    rating_votes_element = soup.select_one('span[property="v:votes"]')
    rating_value = normalize_text(rating_value_element.get_text(" ", strip=True) if rating_value_element else None)
    rating_votes = normalize_text(rating_votes_element.get_text(" ", strip=True) if rating_votes_element else None)

    cover_image = None
    cover = soup.select_one("#mainpic img")
    if cover is not None:
        cover_image = cover.get("src")
    if not cover_image and isinstance(movie_json.get("image"), str):
        cover_image = movie_json.get("image")

    genres = info_data.get("genres") or [
        normalize_text(tag.get_text(strip=True)) for tag in soup.select('span[property="v:genre"]')
    ]
    release_dates = info_data.get("release_dates") or [
        normalize_text(tag.get_text(strip=True)) for tag in soup.select('span[property="v:initialReleaseDate"]')
    ]
    runtimes = info_data.get("runtimes") or [
        normalize_text(tag.get_text(strip=True)) for tag in soup.select('span[property="v:runtime"]')
    ]

    return {
        "subject_id": subject_id,
        "url": url,
        "title": title,
        "year": year,
        "cover_image": cover_image,
        "rating": {
            "value": parse_float(rating_value) or parse_float(str(rating_json.get("ratingValue", ""))),
            "votes": parse_int(rating_votes) or parse_int(str(rating_json.get("ratingCount", ""))),
        },
        "directors": info_data.get("directors") or jsonld_people(movie_json.get("director")),
        "writers": info_data.get("writers") or [],
        "actors": info_data.get("actors") or jsonld_people(movie_json.get("actor")),
        "genres": [item for item in genres if item],
        "countries": info_data.get("countries") or [],
        "languages": info_data.get("languages") or [],
        "release_dates": [item for item in release_dates if item],
        "runtimes": [item for item in runtimes if item],
        "aka": info_data.get("aka") or [],
        "official_site": info_data.get("official_site"),
        "imdb": info_data.get("imdb"),
        "summary": extract_summary(html, movie_json),
        "tags": extract_tags(soup),
    }


def write_output(data: Any, output: str) -> Path:
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path.resolve()


def print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except ValueError:
                pass


class DoubanSpider:
    def __init__(self, timeout: int, sleep_seconds: float, cookie: str | None) -> None:
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        if cookie:
            self.session.headers["Cookie"] = cookie

    def sleep_if_needed(self) -> None:
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)

    def solve_security_check(self, challenge_url: str, html: str) -> None:
        soup = BeautifulSoup(html, "html.parser")
        form = soup.select_one("form#sec")
        if form is None:
            raise RuntimeError("命中了豆瓣风控页，但未找到校验表单。")

        payload = {"tok": "", "cha": "", "sol": "", "red": ""}
        for field in payload:
            element = form.select_one(f'input[name="{field}"], input#{field}')
            payload[field] = element.get("value", "") if element else ""

        if not payload["tok"] or not payload["cha"] or not payload["red"]:
            raise RuntimeError("命中了豆瓣风控页，但未能提取挑战参数。")

        payload["sol"] = str(solve_pow(payload["cha"], extract_difficulty(html)))
        response = self.session.post(
            urljoin(challenge_url, "/c"),
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://sec.douban.com",
                "Referer": challenge_url,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()

    def fetch_html(self, url: str) -> str:
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            set_response_encoding(response)
            html = response.text

            if is_security_page(response.url, html):
                self.solve_security_check(challenge_url=response.url, html=html)
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                set_response_encoding(response)
                html = response.text
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            message = f"请求失败: HTTP {status}"
            if status in {403, 418}:
                message += "。豆瓣可能触发了反爬，建议加 `--cookie` 或提高 `--sleep`。"
            raise RuntimeError(message) from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"网络请求失败: {exc}") from exc

        if is_security_page(response.url, html) or any(flag in html for flag in ("异常请求", "输入验证码")):
            raise RuntimeError("豆瓣返回了风控页面，自动校验失败，请改用 `--cookie`。")

        return html

    def crawl_movie(self, subject: str, extra_data: dict[str, Any] | None = None) -> dict[str, Any]:
        subject_id, url = subject_to_url(subject)
        movie = parse_movie_detail(subject_id=subject_id, url=url, html=self.fetch_html(url))
        if extra_data:
            for key, value in extra_data.items():
                if key != "url":
                    movie[key] = value
        return movie

    def collect_top250_entries(self, start_rank: int, count: int) -> list[dict[str, Any]]:
        end_rank = min(250, start_rank + count - 1)
        page_starts = sorted({((rank - 1) // 25) * 25 for rank in range(start_rank, end_rank + 1)})
        entries: list[dict[str, Any]] = []

        for index, page_start in enumerate(page_starts, start=1):
            html = self.fetch_html(f"{TOP250_URL}?start={page_start}")
            page_entries = extract_top250_entries(html)
            entries.extend(
                entry
                for entry in page_entries
                if entry.get("top250_rank") and start_rank <= entry["top250_rank"] <= end_rank
            )
            if index < len(page_starts):
                self.sleep_if_needed()

        entries.sort(key=lambda item: item["top250_rank"])
        return entries

    def crawl_top250(self, start_rank: int, count: int, output: str | None) -> list[dict[str, Any]]:
        entries = self.collect_top250_entries(start_rank=start_rank, count=count)
        results: list[dict[str, Any]] = []
        total = len(entries)

        for index, entry in enumerate(entries, start=1):
            movie = self.crawl_movie(entry["subject_id"], extra_data=entry)
            results.append(movie)
            print(
                f"[{index}/{total}] TOP{entry['top250_rank']:03d} {movie['title']}",
                file=sys.stderr,
            )
            if output:
                write_output(results, output)
            if index < total:
                self.sleep_if_needed()

        return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取豆瓣电影详情页，或批量抓取豆瓣 TOP250 详情信息。")
    parser.add_argument("subjects", nargs="*", help="豆瓣 subject ID，或豆瓣电影详情页 URL。")
    parser.add_argument("--top250", action="store_true", help="抓取豆瓣 TOP250 的电影详情信息。")
    parser.add_argument("--top250-start", type=int, default=1, help="TOP250 起始排名，默认 1。")
    parser.add_argument("--top250-count", type=int, default=250, help="TOP250 抓取数量，默认 250。")
    parser.add_argument("-o", "--output", help="输出 JSON 文件路径。")
    parser.add_argument("--cookie", help="可选。遇到 403、验证码或风控页时，把浏览器里的 Cookie 字符串粘进来。")
    parser.add_argument("--timeout", type=int, default=15, help="单次请求超时时间，默认 15 秒。")
    parser.add_argument("--sleep", type=float, default=1.5, help="批量抓取时每次请求之间的间隔秒数，默认 1.5 秒。")
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.top250 and args.subjects:
        parser.error("使用 `--top250` 时不要再传 subject ID 或 URL。")
    if not args.top250 and not args.subjects:
        parser.error("请传入 subject ID/URL，或者使用 `--top250`。")
    if args.top250_start < 1 or args.top250_start > 250:
        parser.error("`--top250-start` 必须在 1 到 250 之间。")
    if args.top250_count < 1:
        parser.error("`--top250-count` 必须大于 0。")


def main() -> int:
    configure_stdio()
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)

    spider = DoubanSpider(timeout=args.timeout, sleep_seconds=args.sleep, cookie=args.cookie)

    try:
        if args.top250:
            output = args.output or DEFAULT_TOP250_OUTPUT
            results = spider.crawl_top250(
                start_rank=args.top250_start,
                count=args.top250_count,
                output=output,
            )
            saved_to = write_output(results, output)
            print(f"已抓取 {len(results)} 部 TOP250 电影详情，保存到: {saved_to}")
            return 0

        results = []
        total = len(args.subjects)
        for index, subject in enumerate(args.subjects, start=1):
            results.append(spider.crawl_movie(subject))
            if index < total:
                spider.sleep_if_needed()

        payload = results[0] if len(results) == 1 else results
        if args.output:
            write_output(payload, args.output)
        print_json(payload)
        return 0
    except KeyboardInterrupt:
        print("已中断抓取。若指定了输出文件，已抓取的数据可能已经写入文件。", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"错误: {exc}", file=sys.stderr)
        if args.top250 and (args.output or DEFAULT_TOP250_OUTPUT):
            output = Path(args.output or DEFAULT_TOP250_OUTPUT).resolve()
            print(f"已抓取的部分数据可能已经写入: {output}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
