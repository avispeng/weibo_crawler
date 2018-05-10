import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import re
from config import *
import pymongo
import time

client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]
# pre-compiled pattern
repost_pattern = re.compile(r'<span class="cmt">转发理由:</span>\s+(\S.*?)\s+<!-- 是否进行翻译 -->', re.S)
original_author_pattern = re.compile(r'转发了 <a href=".*?">(.*?)</a>', re.S)
sub_a_pattern = re.compile('<a\shref=.*?>')
comment_pattern = re.compile('^C_') # id of comment starts with C_

PROXY = None
PROXIES = None

def get_proxy():
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

def crawl():
    url = '{url}/{userid}'.format(url=START_URL, userid=USERID)
    # get the total number of pages
    response_first = requests.get(url, headers=HEADERS).text
    pages = get_total_pages(response_first)
    print('the total number of pages is ' + str(pages))

    for page in range(1, pages+1):
        try:
            get_proxy()
            response = requests.get(url + '?page=' + str(page), headers=HEADERS, proxies=PROXIES)
            if response.status_code == 200:
                process_one_page(response.text)
                print("finishing page " + str(page))
                time.sleep(60)
        except RequestException:
            print('request error when requesting page '+str(page))


def get_total_pages(response):
    soup = BeautifulSoup(response, 'lxml')
    pages = soup.find("input", {"name":"mp"})
    if pages:
        return int(pages['value'])
    return None

def process_one_page(response):
    # get into each post's comment url so that we can access complete post content and its comments
    soup = BeautifulSoup(response, 'lxml')
    # posts = soup.find_all("div", {"class": "c"})
    posts = soup.select('.c .cc')
    # exclude comments from original post when it is a repost/forward
    posts = [post['href'] for post in posts if '?uid=' in post['href']] # effective comment urls
    for post in posts:
        if ('http:' in post) and (not 'https:' in post):
            post = post.replace('http', 'https')
        one_post = requests.get(post, headers=HEADERS, timeout=6, proxies=PROXIES)
        process_one_post(one_post.text, post)


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

    pages = get_total_pages(response)
    if not pages:
        pages = 1

    # strip off #cmtfrm
    url = url[:-7] + '&page='
    for page in range(1, pages+1):
        comment_page = requests.get(url+str(page), headers=HEADERS, proxies=PROXIES)
        soup2 = BeautifulSoup(comment_page.text, 'lxml')
        comments_list = soup2.find_all('div', id=comment_pattern)
        if comments_list and len(comments_list) > 0:
            for comment in comments_list:
                comments.append(process_one_comment(comment))
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
        print('save to mongodb successfully: ', result.get('post'))
        return True
    return False

if __name__ == '__main__':
    crawl()