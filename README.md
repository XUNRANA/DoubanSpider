# DoubanSpider

一个可直接运行的豆瓣电影爬虫，使用 `requests + BeautifulSoup4` 抓取单个电影详情，也支持批量抓取豆瓣 `TOP250` 全量详情。

## 安装依赖

```bash
pip install -r requirements.txt
```

## 功能

- 抓取单个豆瓣电影详情
- 批量抓取豆瓣 `TOP250` 电影详情
- 自动处理豆瓣当前的基础安全校验页
- 支持输出为 JSON 文件
- 抓取 `TOP250` 时支持按排名范围抓取

## 单电影用法

抓取单个电影并打印到终端：

```bash
python douban_movie_spider.py 1292052
```

也可以直接传详情页 URL：

```bash
python douban_movie_spider.py https://movie.douban.com/subject/1292052/
```

保存到文件：

```bash
python douban_movie_spider.py 1292052 -o output.json
```

## TOP250 用法

抓取完整 `TOP250` 详情：

```bash
python douban_movie_spider.py --top250
```

默认会保存到当前目录的 `top250_movies.json`。

指定输出文件：

```bash
python douban_movie_spider.py --top250 -o data/top250.json
```

只抓取一部分排名：

```bash
python douban_movie_spider.py --top250 --top250-start 1 --top250-count 50 -o data/top50.json
python douban_movie_spider.py --top250 --top250-start 101 --top250-count 25 -o data/top101_125.json
```

## 常用参数

```bash
python douban_movie_spider.py --top250 --sleep 2 --timeout 20
```

- `--sleep`: 每次请求之间的间隔秒数，默认 `1.5`
- `--timeout`: 单次请求超时时间，默认 `15`
- `--cookie`: 如果豆瓣返回 `403`、验证码或异常请求页面，可以把浏览器里的 Cookie 带上
- `-o/--output`: 输出 JSON 文件路径

示例：

```bash
python douban_movie_spider.py --top250 --cookie "bid=xxxx; ll=118282; ..."
```

## 输出字段

每部电影会包含这些主要字段：

- `subject_id`
- `url`
- `title`
- `year`
- `cover_image`
- `rating`
- `directors`
- `writers`
- `actors`
- `genres`
- `countries`
- `languages`
- `release_dates`
- `runtimes`
- `aka`
- `official_site`
- `imdb`
- `summary`
- `tags`

在 `TOP250` 模式下，还会额外带上：

- `top250_rank`
- `top250_list_title`
- `top250_other_titles`
- `top250_quote`

## 注意事项

- 豆瓣有反爬策略，抓 `TOP250` 时建议保留默认 `--sleep`，不要高频请求
- 如果中途被拦，优先提高 `--sleep`，必要时加 `--cookie`
- `TOP250` 模式会边抓边写文件，中途失败时，已经抓到的数据通常还在输出文件里
