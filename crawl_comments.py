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

from douban_movie_spider import DoubanSpider, normalize_text, parse_int


DEFAULT_INPUT = Path("data/top250.json")
DEFAULT_REPORT = Path("data/comment_crawl_failures.json")
DEFAULT_COMMENTS_PER_MOVIE = 30
DEFAULT_PAGE_SIZE = 20


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl Douban short comments for movies in data/top250.json."
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
    parser.add_argument(
        "--comments-per-movie",
        type=int,
        default=DEFAULT_COMMENTS_PER_MOVIE,
        help="Number of short comments to keep for each movie.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Douban comments page size. Keep this at 20 unless Douban changes.",
    )
    parser.add_argument(
        "--sort",
        default="new_score",
        choices=["new_score", "time"],
        help="Douban comments sort mode. new_score means popular comments.",
    )
    parser.add_argument(
        "--status",
        default="P",
        help="Douban comments status parameter. P means watched comments.",
    )
    parser.add_argument("--timeout", type=int, default=15, help="Request timeout in seconds.")
    parser.add_argument(
        "--sleep-min",
        type=float,
        default=1.2,
        help="Minimum sleep seconds between comment pages.",
    )
    parser.add_argument(
        "--sleep-max",
        type=float,
        default=2.8,
        help="Maximum sleep seconds between comment pages.",
    )
    parser.add_argument(
        "--movie-sleep-min",
        type=float,
        default=1.5,
        help="Minimum extra sleep seconds between movies.",
    )
    parser.add_argument(
        "--movie-sleep-max",
        type=float,
        default=3.5,
        help="Maximum extra sleep seconds between movies.",
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
        "--limit-movies",
        type=int,
        default=None,
        help="Only process the first N selected movies.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Fetch again even when enough comments already exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show which movies would be crawled.",
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


def extract_user_id(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/people/([^/]+)/?", url)
    return match.group(1) if match else None


def extract_subject_id(movie: dict[str, Any]) -> str:
    subject_id = normalize_text(str(movie.get("subject_id") or ""))
    if subject_id:
        return subject_id

    match = re.search(r"/subject/(\d+)/?", movie.get("url") or "")
    if not match:
        raise ValueError(f"Missing subject_id for movie: {movie.get('title')}")
    return match.group(1)


def parse_rating(rating_element: Tag | None) -> dict[str, Any] | None:
    if rating_element is None:
        return None

    classes = rating_element.get("class") or []
    rating_value = None
    for class_name in classes:
        match = re.fullmatch(r"allstar(\d+)", str(class_name))
        if match:
            rating_value = int(match.group(1)) / 10
            break

    return {
        "value": rating_value,
        "label": normalize_text(rating_element.get("title")) or None,
    }


def parse_comment_item(item: Tag) -> dict[str, Any] | None:
    comment_id = normalize_text(item.get("data-cid"))
    comment = item.select_one(".comment")
    if comment is None:
        return None

    info = comment.select_one(".comment-info")
    user_link = info.select_one("a") if info else item.select_one(".avatar a")
    avatar = item.select_one(".avatar img")
    time_element = item.select_one(".comment-time")
    location_element = item.select_one(".comment-location")
    content_element = item.select_one(".comment-content .short") or item.select_one(".comment-content")
    vote_element = item.select_one(".vote-count")
    rating_element = item.select_one(".rating")
    report = item.select_one(".comment-report")

    user_url = urljoin("https://www.douban.com/", user_link.get("href", "")) if user_link else None
    status = None
    if info:
        for span in info.select("span"):
            if "rating" in (span.get("class") or []) or "comment-time" in (span.get("class") or []):
                continue
            text = normalize_text(span.get_text(" ", strip=True))
            if text:
                status = text
                break

    content = normalize_text(content_element.get_text(" ", strip=True) if content_element else None)
    if not content:
        return None

    return {
        "comment_id": comment_id or None,
        "votes": parse_int(vote_element.get_text(" ", strip=True)) if vote_element else None,
        "user": {
            "id": extract_user_id(user_url),
            "name": normalize_text(user_link.get_text(" ", strip=True)) if user_link else None,
            "url": user_url,
            "avatar": avatar.get("src") if avatar else None,
        },
        "status": status,
        "rating": parse_rating(rating_element),
        "created_at": normalize_text(time_element.get("title") or time_element.get_text(" ", strip=True))
        if time_element
        else None,
        "location": normalize_text(location_element.get_text(" ", strip=True)) if location_element else None,
        "content": content,
        "source_url": report.get("data-url") if report else None,
    }


def build_comments_url(
    subject_id: str,
    start: int,
    page_size: int,
    status: str,
    sort: str,
) -> str:
    return (
        f"https://movie.douban.com/subject/{subject_id}/comments"
        f"?start={start}&limit={page_size}&status={status}&sort={sort}"
    )


def parse_comments_page(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    comments: list[dict[str, Any]] = []

    for item in soup.select(".comment-item"):
        comment = parse_comment_item(item)
        if comment:
            comments.append(comment)

    return comments


def crawl_movie_comments(
    spider: DoubanSpider,
    movie: dict[str, Any],
    comments_per_movie: int,
    page_size: int,
    status: str,
    sort: str,
    sleep_min: float,
    sleep_max: float,
) -> list[dict[str, Any]]:
    subject_id = extract_subject_id(movie)
    comments: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for start in range(0, comments_per_movie, page_size):
        url = build_comments_url(
            subject_id=subject_id,
            start=start,
            page_size=page_size,
            status=status,
            sort=sort,
        )
        page_comments = parse_comments_page(spider.fetch_html(url))
        if not page_comments:
            break

        for comment in page_comments:
            key = comment.get("comment_id") or comment.get("content")
            if key in seen_ids:
                continue
            seen_ids.add(str(key))
            comments.append(comment)
            if len(comments) >= comments_per_movie:
                break

        if len(comments) >= comments_per_movie or len(page_comments) < page_size:
            break

        time.sleep(random.uniform(sleep_min, sleep_max))

    return comments[:comments_per_movie]


def needs_comments(movie: dict[str, Any], comments_per_movie: int, force: bool) -> bool:
    if force:
        return True
    comments = movie.get("comments")
    return not isinstance(comments, list) or len(comments) < comments_per_movie


def select_movies(
    movies: list[dict[str, Any]],
    start_rank: int,
    limit_movies: int | None,
    comments_per_movie: int,
    force: bool,
) -> list[tuple[int, dict[str, Any]]]:
    selected = [
        (index, movie)
        for index, movie in enumerate(movies)
        if (movie.get("top250_rank") or 10**9) >= start_rank
        and needs_comments(movie, comments_per_movie, force)
    ]
    if limit_movies is not None:
        selected = selected[:limit_movies]
    return selected


def validate_args(args: argparse.Namespace) -> None:
    if args.comments_per_movie <= 0:
        raise ValueError("--comments-per-movie must be greater than 0")
    if args.page_size <= 0:
        raise ValueError("--page-size must be greater than 0")
    if args.sleep_min > args.sleep_max:
        raise ValueError("--sleep-min cannot be greater than --sleep-max")
    if args.movie_sleep_min > args.movie_sleep_max:
        raise ValueError("--movie-sleep-min cannot be greater than --movie-sleep-max")


def main() -> int:
    configure_stdio()
    args = parse_args()
    validate_args(args)

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path
    report_path = Path(args.report)
    movies = read_movies(input_path)
    selected = select_movies(
        movies=movies,
        start_rank=args.start_rank,
        limit_movies=args.limit_movies,
        comments_per_movie=args.comments_per_movie,
        force=args.force,
    )

    print(f"Movies loaded: {len(movies)}")
    print(f"Movies selected: {len(selected)}")
    print(f"Comments per movie: {args.comments_per_movie}")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")

    if args.dry_run:
        for _, movie in selected[:30]:
            current = len(movie.get("comments") or [])
            print(f"TOP{movie.get('top250_rank')}: {movie.get('title')} ({current} existing)")
        if len(selected) > 30:
            print(f"... and {len(selected) - 30} more")
        return 0

    spider = DoubanSpider(timeout=args.timeout, sleep_seconds=0, cookie=args.cookie or None)
    failures: list[dict[str, Any]] = []

    for order, (movie_index, movie) in enumerate(selected, start=1):
        rank = movie.get("top250_rank")
        title = movie.get("title")
        try:
            comments = crawl_movie_comments(
                spider=spider,
                movie=movie,
                comments_per_movie=args.comments_per_movie,
                page_size=args.page_size,
                status=args.status,
                sort=args.sort,
                sleep_min=args.sleep_min,
                sleep_max=args.sleep_max,
            )
            movie["comments"] = comments
            movie["comments_count"] = len(comments)
            movie["comments_sort"] = args.sort
            movie["comments_status"] = args.status
            movie["comments_updated_at"] = datetime.now().isoformat(timespec="seconds")
            print(f"[{order}/{len(selected)}] OK   TOP{rank}: {title} -> {len(comments)} comments")
        except Exception as exc:
            failures.append(
                {
                    "rank": rank,
                    "title": title,
                    "url": movie.get("url"),
                    "reason": f"{exc.__class__.__name__}: {exc}",
                }
            )
            print(f"[{order}/{len(selected)}] FAIL TOP{rank}: {title} -> {exc}")

        movies[movie_index] = movie
        write_movies(output_path, movies)

        if order < len(selected):
            time.sleep(random.uniform(args.movie_sleep_min, args.movie_sleep_max))

    write_failures(report_path, failures)
    print(f"Failures: {len(failures)}")
    if failures:
        print(f"Failure report: {report_path}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
