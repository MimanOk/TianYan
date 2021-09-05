# -*- coding: utf-8 -*-
# Time: 2021.08.22
__author__ = "huoyijun"

import os
import re
import time
import redis
import random
import shutil
import requests
from copy import copy
from hashlib import md5
import multiprocessing
from loguru import logger
from collections.abc import Iterable
from threading import Thread
from threading import RLock as tRLock
from send_email import send_email
from multiprocessing.dummy import Pool as tPool
from multiprocessing import Manager
from multiprocessing import Pool as pPool


# 线程数(同时消耗每个文件中的url数) 24
THREAD_COUNT = 2
# 进程数(一次使用的文件数) 2
PROCESS_COUNT = 2
# 线程池缓冲数(线程流畅度) 10
THREAD_P_WAIT_COUNT = THREAD_COUNT * 2
# 进程池缓冲数(进程流畅度) 32
PROCESS_P_WAIT_COUNT = PROCESS_COUNT * 2

# 是否使用代理
USE_PROXY = True
# PROXIES_API是否可用
USE_PROXY_API = False
# 代理接口，每2秒取一次，%s=[1,2,3,...]
PROXIES_API = "http://http.tiqu.alibabaapi.com/getip?num=%s&type=2&pack=73571&port=11&lb=1&pb=45&regions="
# PROXIES_API = "http://localhost:5555/get?count=%s"

# >>>>>>>>>>>>>>>>>>>>>>
# redis host
REDIS_HOST = 'localhost'
# redis port
REDIS_PORT = 6379
# redis db
REDIS_DB = 0
# redis charset
REDIS_CHARSET = 'utf8'
# url去重
RDS_URLS = "tianyan_urls"
# redis数据库
rds_db = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, charset=REDIS_CHARSET)
# <<<<<<<<<<<<<<<<<<<<<<

# 一个文件最多提取的urls数量
ONE_FILE_URL = 1

# 工作目录
WORK_DIR = os.getcwd()
# local proxy path, default use .\local_proxies.txt
LOCAL_PROXY_PATH = ''
# 失败url文件名
FAIL_URLS = "fail_urls.txt"
ua_list = []
trace30 = logger.add(os.path.join(WORK_DIR, "logs", "crawl_from_urls.log"), rotation="500 MB", level=30)
with open(os.path.join(os.getcwd(), "ua.txt"), 'r+', encoding='utf-8') as ua_f:
    ua_list_ = ua_f.readlines()
    for ua in ua_list_:
        if isinstance(ua, (bytes,)):
            ua_list.append(ua.decode().strip())
        else:
            ua_list.append(ua.strip())


# local proxy path, default use .\local_proxies.txt
LOCAL_PROXY_PATH = ''
# 保存路径
store_path = os.path.join(WORK_DIR, "CompanyInfo")
# 数据来源路径
from_path = os.path.join(WORK_DIR, "CompanyUrlsT")
# 放到待解析文件夹
back_path = os.path.join(WORK_DIR, "BackUrls")
# 判断文件是否存在，不存在则创建
if not os.path.exists(store_path):
    os.makedirs(store_path)
if not os.path.exists(from_path):
    os.makedirs(from_path)
if not os.path.exists(back_path):
    os.makedirs(back_path)

headers = {
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'cache.baiducontent.com',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': random.choice(ua_list),
}


# 获取接口代理
def get_api_proxy(count=None, api=None):
    if not api:
        logger.warning("未提供代理api接口")
        return []
    if not count:
        count = 1
    try:
        js = requests.get(api % count).json()
    except Exception as e:
        logger.warning("代理api接口有问题：", e)
        return []
    proxy_list = []
    try:
        while "再试" in js['msg']:
            time.sleep(2)
            js = requests.get(api % count).json()
        if "已用完" in js['msg']:
            logger.warning("接口ip已用完！！")
            return []
        if not js['data']:
            logger.warning("接口ip已用完！！")
            return []
        for item in js['data']:
            proxy_list.append(item['ip'] + ':' + item['port'])
        # 代理IP不足则返回少量代理IP
        if len(proxy_list) < count:
            logger.warning("api proxies is too few !    '%s'" % (api % count, ))
            return proxy_list
        return proxy_list
    except Exception as e:
        logger.info(e)
        return []


# 获取本地代理
def get_local_proxy(count=None, path=None):
    if not path:
        path = os.path.join(WORK_DIR, "local_proxies.txt")  # 默认本地代理路径
    elif not os.path.exists(path):  # 判断文件是否存在，不存在则返回1
        logger.warning("proxies file '%s' not exists !" % path)
        return []
    if not count:
        count = 1
    try:
        with open(path, 'r', encoding='utf-8') as f:
            proxies = f.readlines()
    except Exception as e:
        logger.warning(e)
        return []
    proxy_list = []
    if proxies:
        if len(proxies) < count:
            logger.warning("local proxies is too few !    '%s'" % path)
        for proxy in random.choices(proxies, k=count):
            proxy = re.compile(r'\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{1,5}').search(proxy)
            if proxy:
                try:
                    proxy_list.append(proxy.group(0).strip())
                except Exception as e:
                    logger.info(e)
        return proxy_list
    else:
        logger.warning("local proxy is empty !    '%s'" % path)
        return []


def store_html(html, store_path, file_name, t_lock):
    # 写入数据
    try:
        t_lock.acquire()
        with open(os.path.join(store_path, file_name), 'ab+') as f:
            logger.info("%s 保存中" % file_name[:-4])
            f.write(html.content + b'\n')
        t_lock.release()
        return True
    except Exception as e:
        logger.info("%s 无写入" % e)
        return False


def crawl_url(url, item, file_name, t_lock, proxy=None):
    global headers
    header = headers.copy()
    error_count = 20
    t_lock1 = tRLock()
    while error_count:
        if proxy:
            header['User-Agent'] = random.choice(ua_list)
            html = requests.get(url, headers=header, proxies=proxy, timeout=5)
            if not html or html.status_code != 200:
                if error_count > 1:
                    error_count -= 1
                    if USE_PROXY_API:
                        ip_list = get_api_proxy(1, PROXIES_API)
                    else:
                        ip_list = get_local_proxy(1)
                    if ip_list:
                        proxy = ip_list.pop()
                    else:
                        logger.warning("ip获取失败，请检查代理ip")
                        ip_list = get_local_proxy(1)
                        if ip_list:
                            proxy = ip_list.pop()
                    continue
                else:
                    logger.warning("抓取失败: %s" % url[:100])
                    store_url(WORK_DIR, FAIL_URLS, url, t_lock)
                    break
            else:
                if store_html(html, store_path, file_name, t_lock):
                    t_lock.acquire()
                    item[0] += 1
                    t_lock.release()
                    if USE_PROXY_API:
                        back_proxy(proxy.split('//')[-1], 200, t_lock1)
                    break
                elif error_count > 1:
                    error_count -= 1
                    continue
                else:
                    break
        else:
            header['User-Agent'] = random.choice(ua_list)
            html = requests.get(url, headers=header, timeout=5)
            if not html or html.status_code != 200:
                if error_count > 1:
                    error_count -= 1
                    continue
                else:
                    logger.warning("抓取失败: %s" % url[:100])
                    store_url(WORK_DIR, FAIL_URLS, url, t_lock)
                    break
            else:
                if store_html(html, store_path, file_name, t_lock1):
                    t_lock.acquire()
                    item[0] += 1
                    t_lock.release()


def crawl_file(file, item, t_lock1, t_lock2):
    global USE_PROXY
    global USE_PROXY_API
    if os.path.isfile(file):
        file_name = os.path.split(file)[-1]
        with open(file, 'r', encoding='utf-8') as f:
            _urls = f.readlines()
            if _urls:
                urls = []
                for url in _urls:
                    if url:
                        urls.append(url)
                urls_len = len(_urls)
                if urls_len > ONE_FILE_URL:
                    urls_len = ONE_FILE_URL
                if USE_PROXY:
                    if USE_PROXY_API:
                        ip_list = get_api_proxy(urls_len, PROXIES_API)
                        if not ip_list or len(ip_list) < urls_len:
                            logger.warning("代理接口没有ip或IP不足！！")
                            USE_PROXY_API = False
                            if ip_list:
                                for ip in ip_list:
                                    back_proxy(ip, 200, t_lock1)
                            return None
                    else:
                        ip_list = get_local_proxy(urls_len)
                        if not ip_list or len(ip_list) < urls_len:
                            logger.warning("本地文件没有ip或ip不足！！")
                            logger.info("尝试接口ip...")
                            time.sleep(2)
                            USE_PROXY_API = True
                            return None

                    # 拿着urls和对应proxies去请求网页源代码
                    t_pool = tPool(THREAD_COUNT)
                    for url, proxy in zip(urls[:urls_len], ip_list):
                        if rds_db.sadd(RDS_URLS, get_md5(url)):
                            new_proxy = {'https:': 'https://' + proxy}
                            t_pool.apply_async(func=crawl_url, args=(url, item, file_name, t_lock2, new_proxy))
                        else:
                            if USE_PROXY_API:
                                back_proxy(proxy, 200, t_lock2)
                else:
                    # 拿着urls和对应proxies去请求网页源代码
                    t_pool = tPool(THREAD_COUNT)
                    for url in urls:
                        t_pool.apply_async(func=crawl_url, args=(url, item, t_lock2))
                t_pool.close()
                t_pool.join()

        try:
            shutil.move(file, back_path)
        except Exception as e:
            if "already" in str(e):
                try:
                    os.remove(file)
                except Exception as e:
                    logger.warning("remove fail >> %s" % file)
            else:
                logger.info(e)


def crawl_files(url_files, p_queue):
    item = [0]
    files = copy(url_files)
    t_lock1 = t_lock2 = tRLock()
    while True:
        if files:
            t_pool = tPool(THREAD_COUNT)
            if len(files) < THREAD_P_WAIT_COUNT:
                file_count = len(files)
            else:
                file_count = THREAD_P_WAIT_COUNT
            for i in range(file_count):
                try:
                    new_file = files.pop().strip()
                    if new_file:
                        try:
                            t_pool.apply_async(func=crawl_file, args=(new_file, item, t_lock1, t_lock2))
                        except Exception as e:
                            logger.info(e)
                            continue
                except Exception as e:
                    logger.info(e)
                    break

            t_pool.close()
            t_pool.join()
        else:
            break
    p_queue.put(item[0])


# 回收代理
def back_proxy(proxy, status_code, lock):
    store_path = os.path.join(WORK_DIR, "proxy")
    # 判断文件是否存在u，不存在则创建
    if not os.path.exists(store_path):
        os.makedirs(store_path)
    if (status_code < 401 or status_code > 408) and status_code not in [302, 301]:
        # 回收
        lock.acquire()
        with open(os.path.join(store_path, "back_proxies.txt"), 'a+', encoding='utf-8') as f:
            f.write(proxy + '\n')
        lock.release()
    elif status_code in [302, 301]:
        # 回收，待定
        lock.acquire()
        with open(os.path.join(store_path, "back_proxies_302.txt"), 'a+', encoding='utf-8') as f1:
            f1.write(proxy + '\n')
        lock.release()


def store_url(work_dir, filename, url, lock):
    lock.acquire()
    with open(os.path.join(work_dir, filename), 'a+', encoding='utf-8') as f:
        f.write(url + '\n')
    lock.release()


def get_md5(data):
    if isinstance(data, str):
        data = data.encode('utf-8')
    m = md5()
    m.update(data)
    return m.hexdigest()


def monitor(p_queue):
    total = rds_db.scard(RDS_URLS)
    while True:
        try:
            temp = p_queue.get(timeout=15)
        except Exception as e:
            temp = ''
            logger.info("监控线程未能收到来自子进程的消息...")
        if temp == 'end':
            logger.info("采集结束，监控到总采集数据量为: %s" % total)
            break
        if temp:
            total += temp
            logger.info("当前采集数据量: %s" % total)
            time.sleep(1)


if __name__ == '__main__':
    start_time = time.time()
    multiprocessing.freeze_support()

    p_lock = Manager().Lock()
    p_queue = Manager().Queue()

    # 监控
    t = Thread(target=monitor, args=(p_queue,))
    t.start()

    while True:
        files = os.listdir(from_path)
        file_cur = 0
        if files:
            p_pool = pPool(PROCESS_COUNT)
            file_len = len(files)
            for i in range(PROCESS_P_WAIT_COUNT):
                if file_cur >= file_len:
                    break
                if file_len <= THREAD_P_WAIT_COUNT:
                    file_count = file_len
                else:
                    file_count = THREAD_P_WAIT_COUNT
                try:
                    url_files = [os.path.join(from_path, file) for file in files[file_cur: file_cur + file_count]]
                    p_pool.apply_async(func=crawl_files, args=(url_files, p_queue))
                    file_cur += file_count
                except Exception as e:
                    logger.warning(e)
            p_pool.close()
            p_pool.join()
        else:
            logger.info("无文件")
            time.sleep(5)
    # p_queue.put('end')
    # t.join()
    # end_time = time.time()
    # minute, second = divmod((end_time - start_time), 60)
    # logger.info("\n总耗时: %s 分 : %s 秒\n数据采集完成!!" % (minute, round(second, 2)), "-" * 50, sep='\n')
