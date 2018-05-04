# weibo_crawler
**Updated on 2018-5-4**

The program can be used to scrape all the posts and comments of the posts(only text) from one user(different from the logged-in one) and store them in MongoDB. 

Crawling only 10 pages of posts has been tested for now(with one cookie and one IP, pause for 60 seconds after scraping each page. Not sure to what extend it will cause blocking.)

**Python version: 3.6**

**Libraries needed: requests, BeautifulSoup, pymongo**

**A 'config.py' should be added**
```
HEADERS= {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Cookie': '',
    'Host': 'weibo.cn',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': ''
}

USERID = ''
START_URL = 'https://weibo.cn'

MONGO_URL='localhost'
MONGO_DB='weibo'
MONGO_TABLE=USERID
```
Edit Cookie, User-Agent, USERID and any other fields accordingly.
