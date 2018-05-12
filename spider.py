import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import re
from config import *
import pymongo
import ast
from fake_useragent import UserAgent
import multiprocessing
from multiprocessing import Pool, Manager


HEADERS = {
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
client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]
ua = UserAgent(fallback='Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:59.0) Gecko/20100101 Firefox/59.0')
# pre-compiled pattern
repost_pattern = re.compile(r'<span class="cmt">转发理由:</span>\s+(\S.*?)\s+<!-- 是否进行翻译 -->', re.S)
original_author_pattern = re.compile(r'转发了 <a href=".*?">(.*?)</a>', re.S)
sub_a_pattern = re.compile('<a\shref=.*?>')
comment_pattern = re.compile('^C_') # id of comment starts with C_

PROXY = None
PROXIES = None
ERROR_TIMES = 0
LOCK = None


def get_cookie():
    global ERROR_TIMES, HEADERS
    try:
        response = requests.get(COOKIE_POOL_URL)
        if response.status_code == 200:
            cookie_dict = ast.literal_eval(response.text)
            cookie_string = "; ".join([str(x)+"="+str(y) for x,y in cookie_dict.items()])
            HEADERS['Cookie'] = cookie_string
        else:
            get_cookie()
    except ConnectionError as e:
        LOCK.acquire()
        print('error occurred when getting cookies ', e.args)
        print('no valid cookies')
        LOCK.release()
        ERROR_TIMES += 1
        if ERROR_TIMES >= MAX_ERRORS:
            exit(0)
        else:
            get_cookie()


def get_proxy():
    global ERROR_TIMES
    global PROXY
    global PROXIES
    try:
        response = requests.get(PROXY_POOL_URL)
        if response.status_code == 200:
            PROXY = response.text
            PROXIES = {
                'http': 'http://' + PROXY
            }
            print('Using proxy ' + PROXY)
        else:
            get_proxy()
    except ConnectionError as e:
        print('error occurred when getting proxies ', e.args)
        PROXY = None
        PROXIES = None
        ERROR_TIMES += 1
        if ERROR_TIMES >= MAX_ERRORS:
            exit(0)
        else:
            get_proxy()

def get_ua():
    global HEADERS
    HEADERS['User-Agent'] = ua.random


def get_total_pages(response):
    soup = BeautifulSoup(response, 'lxml')
    pages = soup.find("input", {"name":"mp"})
    if pages:
        return int(pages['value'])
    return None


def process_one_page(url, lock):
    global LOCK
    LOCK = lock
    get_proxy()
    get_cookie()
    get_ua()
    while True:
        try:
            response = requests.get(url, headers=HEADERS, proxies=PROXIES)
            if response.status_code == 200:
                response_text = response.text
                # get into each post's comment url so that we can access complete post content and its comments
                soup = BeautifulSoup(response_text, 'lxml')
                # posts = soup.find_all("div", {"class": "c"})
                posts = soup.select('.c .cc')
                # exclude comments from original post when it is a repost/forward
                posts = [post['href'] for post in posts if '?uid=' in post['href']]  # effective comment urls
                for post in posts:
                    if ('http:' in post) and (not 'https:' in post):
                        post = post.replace('http', 'https')
                    while True:
                        one_post = requests.get(post, headers=HEADERS, timeout=15, proxies=PROXIES)
                        if one_post.status_code == 200:
                            process_one_post(one_post.text, post)
                        else:
                            get_proxy()
                            get_cookie()
                            get_ua()
                            continue
                        break
            else:
                LOCK.acquire()
                print('get status code ' + str(response.status_code) + ' when crawling page ' + url)
                LOCK.release()
                get_proxy()
                get_cookie()
                get_ua()
                continue
            break
        except RequestException:
            LOCK.acquire()
            print('request error when requesting page ' + url)
            LOCK.release()
            get_proxy()
            get_cookie()
            get_ua()
            continue


def process_one_post(response, url):
    soup = BeautifulSoup(response, 'lxml')
    post = soup.select_one('#M_')
    post_content = post.select_one('.ctt').get_text()
    # if it is a repost
    repost_reason = re.findall(repost_pattern, str(post))
    if len(repost_reason) > 0:
        repost = repost_reason[0]
        # delete hyperlinks to other uses' page in repost reason
        repost = re.sub(sub_a_pattern, '', repost)
        # repost = re.sub('</a>', ' ', repost)
        repost = repost.replace('</a>', '')
        original_author = re.findall(original_author_pattern, str(post))
        if len(original_author) > 0:
            ori_author = original_author[0] + ":"
        else:
            # possibly the original post is deleted
            ori_author = ""
        post_content = "转发理由:" + repost + '//原博:' + ori_author + post_content

    post_timestamp = post.select_one('.ct').get_text()
    comments = [] # a list of dictionaries with author, content, and timestamp as fields
    if CRAWL_COMMENTS:
        pages = get_total_pages(response)
        if not pages:
            pages = 1
        # strip off #cmtfrm
        url = url[:-7] + '&page='
        for page in range(1, pages+1):
            while True:
                comment_page = requests.get(url+str(page), headers=HEADERS, proxies=PROXIES)
                if comment_page.status_code == 200:
                    soup2 = BeautifulSoup(comment_page.text, 'lxml')
                    comments_list = soup2.find_all('div', id=comment_pattern)
                    if comments_list and len(comments_list) > 0:
                        for comment in comments_list:
                            comments.append(process_one_comment(comment))
                else:
                    LOCK.acquire()
                    print('get status code ' + str(comment_page.status_code) + ' when crawling comment page ' + url+str(page))
                    LOCK.release()
                    get_proxy()
                    get_cookie()
                    get_ua()
                    continue
                break
    result = {
        'post': post_content,
        'timestamp': post_timestamp,
        'comments': comments
    }
    save_to_mongo(result)


def process_one_comment(comment):
    # soup = BeautifulSoup(comment, 'lxml')
    soup = comment
    author = soup.a.string
    content = soup.select_one('.ctt').get_text()
    timestamp_source = soup.select_one('.ct').get_text()
    return {
        'author': author,
        'content': content,
        'timestamp_source': timestamp_source
    }


def save_to_mongo(result):
    if db[MONGO_TABLE].insert(result):
        LOCK.acquire()
        print('save to mongodb successfully: ', result.get('post'))
        LOCK.release()
        return True
    return False


def main():
    global LOCK
    manager = Manager()
    LOCK = manager.Lock()
    pool = Pool(multiprocessing.cpu_count())

    url = '{url}/{userid}'.format(url=START_URL, userid=USERID)
    # get the total number of pages
    get_cookie()
    get_proxy()
    get_ua()
    response_first = requests.get(url, headers=HEADERS, proxies=PROXIES).text
    pages = get_total_pages(response_first)
    print('the total number of pages to crawl is ' + str(pages - START_PAGE))
    print('starting...')
    for page in range(START_PAGE, pages+1):
        # change user agent, proxy and cookie crawling each page
        pool.apply_async(process_one_page, args=(url+'?page='+str(page), LOCK))
        LOCK.acquire()
        print('finishing assigning page '+str(page))
        LOCK.release()
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()