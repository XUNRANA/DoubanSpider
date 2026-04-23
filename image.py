import argparse
import json
import os
import random
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests


DEFAULT_DATA_GLOB = "top250*.json"
DEFAULT_DATA_DIR = Path("data")
DEFAULT_OUTPUT_DIR = Path("images")
DEFAULT_REPORT_PATH = DEFAULT_DATA_DIR / "poster_failures.json"
DEFAULT_MIN_SIZE = 5_000
DOUBAN_IMAGE_HOSTS = (
    "img1.doubanio.com",
    "img2.doubanio.com",
    "img3.doubanio.com",
    "img9.doubanio.com",
)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
]
INVALID_FILENAME_CHARS = r'\/:*?"<>|'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill in missing Douban movie posters from local TOP250 JSON files."
    )
    parser.add_argument(
        "--json",
        nargs="*",
        default=None,
        help="JSON files or directories to scan. Default: auto-discover data/top250*.json.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory used to store poster images.",
    )
    parser.add_argument(
        "--report",
        default=str(DEFAULT_REPORT_PATH),
        help="Path of the JSON failure report.",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=DEFAULT_MIN_SIZE,
        help="Minimum file size in bytes for a poster to be treated as valid.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=12,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retry count for each candidate poster URL.",
    )
    parser.add_argument(
        "--sleep-min",
        type=float,
        default=1.0,
        help="Minimum sleep interval between retries and movies.",
    )
    parser.add_argument(
        "--sleep-max",
        type=float,
        default=2.0,
        help="Maximum sleep interval between retries and movies.",
    )
    parser.add_argument(
        "--cookie",
        default=os.getenv("DOUBAN_COOKIE", ""),
        help="Optional Douban cookie used when image requests are blocked.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N missing posters.",
    )
    parser.add_argument(
        "--redownload-all",
        action="store_true",
        help="Redownload posters even when a valid local file already exists.",
    )
    parser.add_argument(
        "--keep-bad-files",
        action="store_true",
        help="Do not delete tiny broken image files before retrying.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which posters are missing without downloading anything.",
    )
    return parser.parse_args()


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="backslashreplace")


def expand_json_inputs(raw_inputs: list[str] | None) -> list[Path]:
    if not raw_inputs:
        return sorted(DEFAULT_DATA_DIR.glob(DEFAULT_DATA_GLOB))

    paths: list[Path] = []
    for raw_input in raw_inputs:
        path = Path(raw_input)
        if path.is_dir():
            discovered = sorted(path.glob(DEFAULT_DATA_GLOB))
            if not discovered:
                discovered = sorted(path.glob("*.json"))
            paths.extend(discovered)
        else:
            paths.append(path)

    unique_paths: list[Path] = []
    seen = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_paths.append(path)
    return unique_paths


def load_movies(json_paths: list[Path]) -> list[dict]:
    movies: list[dict] = []
    seen_keys = set()

    for path in json_paths:
        if not path.exists():
            raise FileNotFoundError(f"JSON file not found: {path}")
        raw_data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw_data, list):
            raise ValueError(f"JSON root must be a list: {path}")

        for movie in raw_data:
            if not isinstance(movie, dict):
                continue
            key = movie.get("subject_id") or movie.get("top250_rank") or movie.get("title")
            if key in seen_keys:
                continue
            seen_keys.add(key)
            movies.append(movie)

    return sorted(movies, key=lambda item: item.get("top250_rank") or 10**9)


def sanitize_title(title: str | None) -> str:
    raw = title or ""
    cleaned = "".join(char for char in raw if char not in INVALID_FILENAME_CHARS)
    return cleaned or "untitled"


def poster_path(movie: dict, output_dir: Path) -> Path:
    rank = movie.get("top250_rank") or "X"
    title = sanitize_title(movie.get("title"))
    return output_dir / f"{rank}_{title}.jpg"


def build_headers(cookie: str = "") -> dict[str, str]:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://movie.douban.com/",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def add_candidate(url: str, seen: set[str], candidates: list[str]) -> None:
    if url and url not in seen:
        seen.add(url)
        candidates.append(url)


def build_ratio_variants(url: str) -> list[str]:
    if "s_ratio_poster" in url:
        return [url.replace("s_ratio_poster", "l_ratio_poster"), url]
    if "l_ratio_poster" in url:
        return [url, url.replace("l_ratio_poster", "s_ratio_poster")]
    return [url]


def build_candidate_urls(url: str) -> list[str]:
    seen: set[str] = set()
    candidates: list[str] = []

    for ratio_url in build_ratio_variants(url):
        parsed = urlsplit(ratio_url)
        add_candidate(ratio_url, seen, candidates)

        if parsed.netloc.endswith("doubanio.com"):
            for host in DOUBAN_IMAGE_HOSTS:
                if host == parsed.netloc:
                    continue
                mirrored = urlunsplit(
                    (parsed.scheme, host, parsed.path, parsed.query, parsed.fragment)
                )
                add_candidate(mirrored, seen, candidates)

    return candidates


def remove_bad_images(folder: Path, min_size: int) -> int:
    removed = 0
    if not folder.exists():
        return removed

    for path in folder.glob("*.jpg"):
        if path.is_file() and path.stat().st_size < min_size:
            path.unlink()
            removed += 1
    return removed


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_atomic(path: Path, content: bytes) -> None:
    tmp_path = path.with_suffix(path.suffix + ".part")
    ensure_parent(path)
    tmp_path.write_bytes(content)
    tmp_path.replace(path)


def collect_missing_movies(
    movies: list[dict],
    output_dir: Path,
    min_size: int,
    redownload_all: bool,
) -> tuple[list[dict], int]:
    pending: list[dict] = []
    valid_existing = 0

    for movie in movies:
        image_url = movie.get("cover_image")
        if not image_url:
            pending.append(
                {
                    "movie": movie,
                    "path": poster_path(movie, output_dir),
                    "reason": "missing cover_image in JSON",
                }
            )
            continue

        path = poster_path(movie, output_dir)
        if not redownload_all and path.exists() and path.stat().st_size >= min_size:
            valid_existing += 1
            continue

        if path.exists() and path.stat().st_size < min_size:
            path.unlink()

        pending.append({"movie": movie, "path": path, "reason": "missing local file"})

    return pending, valid_existing


def download_image(
    session: requests.Session,
    url: str,
    destination: Path,
    timeout: float,
    retries: int,
    min_size: int,
    sleep_min: float,
    sleep_max: float,
    cookie: str,
) -> tuple[bool, str]:
    last_error = "no candidate URL tried"

    for candidate in build_candidate_urls(url):
        for attempt in range(1, retries + 1):
            try:
                response = session.get(
                    candidate,
                    headers=build_headers(cookie),
                    timeout=timeout,
                    allow_redirects=True,
                )
                content_type = response.headers.get("Content-Type", "").lower()

                if response.status_code != 200:
                    last_error = f"HTTP {response.status_code} from {candidate}"
                elif "image" not in content_type:
                    last_error = f"non-image response {content_type or 'unknown'} from {candidate}"
                elif len(response.content) < min_size:
                    last_error = f"image too small ({len(response.content)} bytes) from {candidate}"
                else:
                    write_atomic(destination, response.content)
                    return True, f"downloaded from {candidate}"
            except requests.RequestException as exc:
                last_error = f"{exc.__class__.__name__} from {candidate}: {exc}"

            if attempt < retries:
                time.sleep(random.uniform(sleep_min, sleep_max))

    return False, last_error


def write_report(report_path: Path, failures: list[dict]) -> None:
    ensure_parent(report_path)
    report_path.write_text(
        json.dumps(failures, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    configure_stdio()
    args = parse_args()
    output_dir = Path(args.output_dir)
    report_path = Path(args.report)

    if args.sleep_min > args.sleep_max:
        raise ValueError("--sleep-min cannot be greater than --sleep-max")

    json_paths = expand_json_inputs(args.json)
    if not json_paths:
        print("No JSON input files found.")
        return 1

    movies = load_movies(json_paths)
    output_dir.mkdir(parents=True, exist_ok=True)

    removed_bad = 0
    if not args.keep_bad_files:
        removed_bad = remove_bad_images(output_dir, args.min_size)

    pending, valid_existing = collect_missing_movies(
        movies=movies,
        output_dir=output_dir,
        min_size=args.min_size,
        redownload_all=args.redownload_all,
    )

    if args.limit is not None:
        pending = pending[: args.limit]

    print(f"JSON files: {', '.join(str(path) for path in json_paths)}")
    print(f"Movies loaded: {len(movies)}")
    print(f"Valid local posters: {valid_existing}")
    print(f"Removed broken posters: {removed_bad}")
    print(f"Posters to fetch: {len(pending)}")

    if args.dry_run:
        preview = [
            {
                "rank": item["movie"].get("top250_rank"),
                "title": item["movie"].get("title"),
                "file": str(item["path"]),
                "url": item["movie"].get("cover_image"),
                "reason": item["reason"],
            }
            for item in pending
        ]
        write_report(report_path, preview)
        print(f"Dry run only. Pending poster list written to: {report_path}")
        return 0

    failures: list[dict] = []
    downloaded = 0
    session = requests.Session()

    for index, item in enumerate(pending, start=1):
        movie = item["movie"]
        path = item["path"]
        rank = movie.get("top250_rank")
        title = movie.get("title")
        ok, detail = download_image(
            session=session,
            url=movie.get("cover_image", ""),
            destination=path,
            timeout=args.timeout,
            retries=args.retries,
            min_size=args.min_size,
            sleep_min=args.sleep_min,
            sleep_max=args.sleep_max,
            cookie=args.cookie,
        )

        if ok:
            downloaded += 1
            print(f"[{index}/{len(pending)}] OK   TOP{rank}: {title}")
        else:
            print(f"[{index}/{len(pending)}] FAIL TOP{rank}: {title} -> {detail}")
            failures.append(
                {
                    "rank": rank,
                    "title": title,
                    "file": str(path),
                    "url": movie.get("cover_image"),
                    "reason": detail,
                }
            )

        time.sleep(random.uniform(args.sleep_min, args.sleep_max))

    write_report(report_path, failures)
    print(f"Downloaded posters: {downloaded}")
    print(f"Failed posters: {len(failures)}")
    print(f"Failure report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
