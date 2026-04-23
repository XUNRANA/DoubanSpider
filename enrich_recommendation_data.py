#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from douban_movie_spider import (
    DoubanSpider,
    collect_until_br,
    normalize_text,
    parse_float,
    parse_int,
)


DEFAULT_INPUT = Path("data/top250.json")
DEFAULT_REPORT = Path("data/recommendation_enrich_failures.json")
BASIC_INFO_LABELS = {
    "导演",
    "编剧",
    "主演",
    "类型",
    "制片国家/地区",
    "语言",
    "上映日期",
    "片长",
    "又名",
    "官方网站",
    "IMDb",
}
DETAIL_ROLE_FIELDS = {
    "导演": "director_details",
    "编剧": "writer_details",
    "主演": "actor_details",
}


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich existing Douban TOP250 data with recommendation-friendly fields."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input JSON file.")
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON file. Default: overwrite --input incrementally.",
    )
    parser.add_argument(
        "--report",
        default=str(DEFAULT_REPORT),
        help="Failure report path. Removed automatically when there are no failures.",
    )
    parser.add_argument("--timeout", type=int, default=15, help="Request timeout in seconds.")
    parser.add_argument(
        "--sleep-min",
        type=float,
        default=1.2,
        help="Minimum sleep seconds between movies.",
    )
    parser.add_argument(
        "--sleep-max",
        type=float,
        default=2.8,
        help="Maximum sleep seconds between movies.",
    )
    parser.add_argument(
        "--cookie",
        default=os.getenv("DOUBAN_COOKIE", ""),
        help="Optional Douban cookie. Can also be provided through DOUBAN_COOKIE.",
    )
    parser.add_argument(
        "--start-rank",
        type=int,
        default=1,
        help="Only process movies whose TOP250 rank is >= this value.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N selected movies.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Fetch again even when recommendation fields already exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show which movies would be enriched.",
    )
    return parser.parse_args()


def read_movies(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"JSON root must be a list: {path}")
    return data


def write_movies(path: Path, movies: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".part")
    tmp_path.write_text(json.dumps(movies, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def write_failures(path: Path, failures: list[dict[str, Any]]) -> None:
    if failures:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
    elif path.exists():
        path.unlink()


def percent_to_float(value: str | None) -> float | None:
    return parse_float(normalize_text(value).replace("%", ""))


def extract_subject_id(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/subject/(\d+)/?", url)
    return match.group(1) if match else None


def extract_person_id(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/(?:personage|celebrity)/(\d+)/?", url)
    return match.group(1) if match else None


def extract_url_from_style(style: str | None) -> str | None:
    if not style:
        return None
    match = re.search(r"url\((.*?)\)", style)
    if not match:
        return None
    return match.group(1).strip("\"'")


def parse_person_anchor(anchor: Tag) -> dict[str, Any] | None:
    name = normalize_text(anchor.get_text(" ", strip=True))
    href = anchor.get("href")
    if not name or not href:
        return None

    absolute_url = urljoin("https://movie.douban.com/", href)
    item: dict[str, Any] = {
        "id": extract_person_id(absolute_url),
        "name": name,
        "url": absolute_url,
    }

    title = normalize_text(anchor.get("title"))
    if title and title != name:
        item["title"] = title

    return item


def collect_anchors_until_br(label: Tag) -> list[Tag]:
    anchors: list[Tag] = []
    for sibling in label.next_siblings:
        if isinstance(sibling, Tag) and sibling.name == "br":
            break
        if isinstance(sibling, Tag):
            anchors.extend(sibling.select("a"))
    return anchors


def extract_info_fields(soup: BeautifulSoup) -> dict[str, str]:
    fields: dict[str, str] = {}
    info = soup.select_one("#info")
    if info is None:
        return fields

    for label in info.select("span.pl"):
        label_text = normalize_text(label.get_text(strip=True)).rstrip(":：")
        value = collect_until_br(label)
        if label_text and value:
            fields[label_text] = value
    return fields


def extract_role_details(soup: BeautifulSoup) -> dict[str, list[dict[str, Any]]]:
    details: dict[str, list[dict[str, Any]]] = {}
    info = soup.select_one("#info")
    if info is None:
        return details

    for label in info.select("span.pl"):
        label_text = normalize_text(label.get_text(strip=True)).rstrip(":：")
        output_key = DETAIL_ROLE_FIELDS.get(label_text)
        if not output_key:
            continue

        people = []
        for anchor in collect_anchors_until_br(label):
            person = parse_person_anchor(anchor)
            if person:
                people.append(person)
        if people:
            details[output_key] = people

    return details


def extract_rating_breakdown(soup: BeautifulSoup) -> list[dict[str, Any]]:
    breakdown: list[dict[str, Any]] = []

    for item in soup.select(".ratings-on-weight .item"):
        star_element = item.select_one(".starstop")
        percent_element = item.select_one(".rating_per")
        if star_element is None or percent_element is None:
            continue

        label = normalize_text(star_element.get_text(" ", strip=True))
        star_match = re.search(r"(\d+)", label)
        if not star_match:
            continue

        breakdown.append(
            {
                "star": parse_int(star_match.group(1)),
                "label": label,
                "text": normalize_text(star_element.get("title")) or None,
                "percentage": percent_to_float(percent_element.get_text(" ", strip=True)),
            }
        )

    return sorted(breakdown, key=lambda row: row["star"] or 0, reverse=True)


def extract_rating_better_than(soup: BeautifulSoup) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    for anchor in soup.select(".rating_betterthan a"):
        text = normalize_text(anchor.get_text(" ", strip=True))
        match = re.search(r"([\d.]+)%\s*(.+)", text)
        if not match:
            continue

        genre_label = normalize_text(match.group(2))
        items.append(
            {
                "percentage": parse_float(match.group(1)),
                "genre": genre_label.removesuffix("片"),
                "genre_label": genre_label,
                "url": urljoin("https://movie.douban.com/", anchor.get("href", "")),
            }
        )

    return items


def extract_interest_counts(soup: BeautifulSoup) -> dict[str, int]:
    text = soup.get_text(" ", strip=True)
    patterns = {
        "wish": r"([\d,]+)\s*人想看",
        "collect": r"([\d,]+)\s*人看过",
        "doing": r"([\d,]+)\s*人在看",
    }
    counts: dict[str, int] = {}

    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            value = parse_int(match.group(1))
            if value is not None:
                counts[key] = value

    return counts


def extract_related_movies(soup: BeautifulSoup) -> list[dict[str, Any]]:
    related: list[dict[str, Any]] = []

    for item in soup.select("#recommendations .recommendations-bd dl"):
        link = item.select_one("dd a") or item.select_one("dt a")
        if link is None:
            continue

        href = link.get("href")
        url = urljoin("https://movie.douban.com/", href or "")
        image = item.select_one("img")
        rating = item.select_one(".subject-rate")
        title = normalize_text(link.get_text(" ", strip=True))
        if not title and image is not None:
            title = normalize_text(image.get("alt"))
        if not title:
            continue

        related.append(
            {
                "subject_id": extract_subject_id(url),
                "title": title,
                "rating": parse_float(rating.get_text(" ", strip=True)) if rating else None,
                "url": url,
                "cover_image": image.get("src") if image else None,
            }
        )

    return related


def extract_celebrity_preview(soup: BeautifulSoup) -> list[dict[str, Any]]:
    celebrities: list[dict[str, Any]] = []

    for item in soup.select("#celebrities li.celebrity"):
        link = item.select_one("a.name") or item.select_one("a[href*='/personage/']")
        if link is None:
            continue

        href = link.get("href")
        url = urljoin("https://movie.douban.com/", href or "")
        name = normalize_text(link.get_text(" ", strip=True))
        role = item.select_one(".role")
        avatar = item.select_one(".avatar")

        celebrities.append(
            {
                "id": extract_person_id(url),
                "name": name,
                "title": normalize_text(link.get("title")) or None,
                "role": normalize_text(role.get("title") or role.get_text(" ", strip=True)) if role else None,
                "url": url,
                "avatar": extract_url_from_style(avatar.get("style")) if avatar else None,
            }
        )

    return celebrities


def extract_awards(soup: BeautifulSoup) -> list[dict[str, Any]]:
    awards: list[dict[str, Any]] = []

    for award in soup.select("ul.award"):
        rows = award.select("li")
        if not rows:
            continue

        award_link = rows[0].select_one("a")
        award_name = normalize_text(rows[0].get_text(" ", strip=True))
        category = normalize_text(rows[1].get_text(" ", strip=True)) if len(rows) > 1 else ""
        status = None
        if "获奖" in category:
            status = "won"
        elif "提名" in category:
            status = "nominated"
        category_clean = re.sub(r"[（(]\s*(获奖|提名)\s*[)）]", "", category).strip()

        recipients = []
        for row in rows[2:]:
            row_people = [parse_person_anchor(anchor) for anchor in row.select("a")]
            row_people = [person for person in row_people if person]
            if row_people:
                recipients.extend(row_people)
                continue

            text = normalize_text(row.get_text(" ", strip=True))
            if text:
                recipients.append({"name": text})

        award_url = urljoin("https://movie.douban.com/", award_link.get("href", "")) if award_link else None
        awards.append(
            {
                "name": award_name,
                "url": award_url,
                "category": category_clean or category or None,
                "status": status,
                "recipients": recipients,
            }
        )

    return awards


def extract_tags(soup: BeautifulSoup) -> list[str]:
    tags: list[str] = []
    selectors = [
        ".tags-body a",
        "#db-tags-section a[href*='/tag/']",
        "#subject-tags-section a[href*='/tag/']",
        ".subject-tags a[href*='/tag/']",
    ]

    for selector in selectors:
        for anchor in soup.select(selector):
            tag = normalize_text(anchor.get_text(" ", strip=True))
            if tag and tag not in tags:
                tags.append(tag)

    return tags


def extract_episode_fields(info_fields: dict[str, str]) -> dict[str, Any]:
    fields: dict[str, Any] = {}

    if "集数" in info_fields:
        fields["episode_count"] = parse_int(info_fields["集数"])
    if "单集片长" in info_fields:
        fields["single_episode_runtime"] = info_fields["单集片长"]
    if "首播" in info_fields:
        fields["premiere_date"] = info_fields["首播"]

    return {key: value for key, value in fields.items() if value not in (None, "", [], {})}


def extract_features(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    info_fields = extract_info_fields(soup)
    extra_info = {
        key: value
        for key, value in info_fields.items()
        if key not in BASIC_INFO_LABELS
    }

    features: dict[str, Any] = {
        "rating_breakdown": extract_rating_breakdown(soup),
        "rating_better_than": extract_rating_better_than(soup),
        "related_movies": extract_related_movies(soup),
        "celebrity_preview": extract_celebrity_preview(soup),
        "awards": extract_awards(soup),
        "recommendation_features_updated_at": datetime.now().isoformat(timespec="seconds"),
    }

    features.update(extract_role_details(soup))
    features.update(extract_episode_fields(info_fields))

    tags = extract_tags(soup)
    if tags:
        features["tags"] = tags

    interest_counts = extract_interest_counts(soup)
    if interest_counts:
        features["interest_counts"] = interest_counts

    if extra_info:
        features["extra_info"] = extra_info

    return features


def needs_enrichment(movie: dict[str, Any], force: bool) -> bool:
    if force:
        return True
    return not movie.get("recommendation_features_updated_at")


def select_movies(
    movies: list[dict[str, Any]],
    start_rank: int,
    limit: int | None,
    force: bool,
) -> list[tuple[int, dict[str, Any]]]:
    selected = [
        (index, movie)
        for index, movie in enumerate(movies)
        if (movie.get("top250_rank") or 10**9) >= start_rank and needs_enrichment(movie, force)
    ]
    if limit is not None:
        selected = selected[:limit]
    return selected


def main() -> int:
    configure_stdio()
    args = parse_args()

    if args.sleep_min > args.sleep_max:
        raise ValueError("--sleep-min cannot be greater than --sleep-max")

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path
    report_path = Path(args.report)
    movies = read_movies(input_path)
    selected = select_movies(movies, args.start_rank, args.limit, args.force)

    print(f"Movies loaded: {len(movies)}")
    print(f"Movies selected: {len(selected)}")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")

    if args.dry_run:
        for _, movie in selected[:30]:
            print(f"TOP{movie.get('top250_rank')}: {movie.get('title')}")
        if len(selected) > 30:
            print(f"... and {len(selected) - 30} more")
        return 0

    spider = DoubanSpider(timeout=args.timeout, sleep_seconds=0, cookie=args.cookie or None)
    failures: list[dict[str, Any]] = []

    for order, (movie_index, movie) in enumerate(selected, start=1):
        rank = movie.get("top250_rank")
        title = movie.get("title")
        url = movie.get("url")

        try:
            html = spider.fetch_html(url)
            features = extract_features(html)
            movie.update(features)
            print(f"[{order}/{len(selected)}] OK   TOP{rank}: {title}")
        except Exception as exc:
            failures.append(
                {
                    "rank": rank,
                    "title": title,
                    "url": url,
                    "reason": f"{exc.__class__.__name__}: {exc}",
                }
            )
            print(f"[{order}/{len(selected)}] FAIL TOP{rank}: {title} -> {exc}")

        movies[movie_index] = movie
        write_movies(output_path, movies)

        if order < len(selected):
            time.sleep(random.uniform(args.sleep_min, args.sleep_max))

    write_failures(report_path, failures)
    print(f"Failures: {len(failures)}")
    if failures:
        print(f"Failure report: {report_path}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
