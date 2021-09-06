# -*- coding: utf-8 -*-
import os
import re
import time
import random
import parsel
import shutil
import redis
import requests
from copy import copy
from loguru import logger
import multiprocessing
from urllib.parse import quote
from threading import Thread
from threading import RLock as tRLock
from multiprocessing import Manager
from send_email import send_email
from multiprocessing.dummy import Pool as tPool
from multiprocessing import Pool as pPool


# 线程数 32
THREAD_COUNT = 1
# 进程数 2
PROCESS_COUNT = 1
# 线程池缓冲数(线程流畅度) 10
THREAD_P_WAIT_COUNT = THREAD_COUNT * 1
# 进程池缓冲数(进程流畅度) 32
PROCESS_P_WAIT_COUNT = PROCESS_COUNT * 1

USE_PROXY = True
# 接口ip是否能用
USE_PROXY_API = True
# 代理接口
# PROXIES_API = "http://http.tiqu.alibabaapi.com/getip?num=%s&type=2&pack=73571&port=11&lb=1&pb=45&regions="
PROXIES_API = "http://localhost:8790/get?count=%s"
# 一页最多采多少条url
ONE_PAGE_URL = 1

# 允许最大尝试次数
ERROR_COUNT = 20
WORK_DIR = os.getcwd()
# local proxy path, default use .\local_proxies.txt
LOCAL_PROXY_PATH = ''
# 未搜索到结果的公司文件名
NO_COMPANIES = "no_companies.txt"
# 失败公司保存文件名
FAIL_COMPANIES = "fail_companies.txt"
# url初始保存路径
store_path = os.path.join(WORK_DIR, "CompanyUrls")
# url临时保存路径
dest_path = os.path.join(WORK_DIR, "CompanyUrlsT")
# 判断文件是否存在，不存在则创建
if not os.path.exists(store_path):
    os.makedirs(store_path)
if not os.path.exists(dest_path):
    os.makedirs(dest_path)

# >>>>>>>>>>>>>>>>>>>>>>
# redis host
REDIS_HOST = 'localhost'
# redis port
REDIS_PORT = 6379
# redis db
REDIS_DB = 0
# redis charset
REDIS_CHARSET = 'utf8'
# 公司名去重
RDS_COMPANIES = "tianyan_companies"
# redis数据库
rds_db = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, charset=REDIS_CHARSET)
# <<<<<<<<<<<<<<<<<<<<<s<

trace30 = logger.add(os.path.join(WORK_DIR, "logs", "TianYan.log"), rotation="500 MB", level=30)

# User-Agent
ua_list = []
with open(os.path.join(os.getcwd(), "ua.txt"), 'r+', encoding='utf-8') as ua_f:
    ua_list_ = ua_f.readlines()
    for ua in ua_list_:
        if isinstance(ua, (bytes,)):
            ua_list.append(ua.decode().strip())
        else:
            ua_list.append(ua.strip())

base_url = "https://www.baidu.com/s?ie=utf-8&wd=site%3Atianyancha.com%20{keyword}&rn=50&pn={pn}"

headers = {
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh,zh-TW;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'www.baidu.com',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'sec-ch-ua-mobile': '?0',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': random.choice(ua_list),
}


def crawl_pages(company_name, pages, t_lock, item, start_proxy=None):
    global USE_PROXY
    global USE_PROXY_API
    have_data = True
    session = requests.Session()

    for page in range(pages):
        url = base_url.format(keyword=quote(company_name), pn=page * 50)
        html = ''
        proxy = ''
        error_count = ERROR_COUNT
        while error_count > 0:
            try:
                # 请求第一层界面
                if USE_PROXY:
                    headers['User-Agent'] = random.choice(ua_list)
                    session.headers.update(headers)
                    if not proxy:
                        proxy = "https://" + start_proxy
                    else:
                        if USE_PROXY_API:
                            proxy_t = get_api_proxy(1, PROXIES_API)
                            if not proxy_t:
                                logger.warning("代理接口没有ip或IP不足！！")
                                logger.info("启用本地ip...")
                                USE_PROXY_API = False
                                error_count = ERROR_COUNT
                                continue
                        else:
                            proxy_t = get_local_proxy(1)
                            if not proxy_t:
                                logger.warning("本地文件没有ip或ip不足！！")
                                logger.info("启用代理接口...")
                                time.sleep(2)
                                USE_PROXY_API = True
                                continue

                        if proxy_t:
                            proxy = proxy_t[0]
                        else:
                            logger.warning("have no proxy")
                            store_company(WORK_DIR, FAIL_COMPANIES, company_name, t_lock)
                            return None
                        proxy = "https://" + proxy
                    proxies = {
                        'http': proxy.replace('s', ''),
                        'https': proxy,
                    }
                    # proxies = {
                    #     'no_proxy': proxy,
                    # }

                    html = session.get(url, timeout=8, proxies=proxies)
                else:
                    html = session.get(url, timeout=8)
            except Exception as e:
                if "HTTP" in str(e):
                    rds_db.sadd("proxy443", proxy.split("//")[-1])
                elif USE_PROXY_API:
                    back_proxy(proxy.split('//')[-1], 200, t_lock)
                logger.warning("error: %s" % e)
            if not html:
                if error_count > 1:
                    error_count -= 1
                    continue
                else:
                    logger.info("抓取失败: %s" % company_name)
                    store_company(WORK_DIR, FAIL_COMPANIES, company_name, t_lock)
                    have_data = False
                    if not USE_PROXY_API:
                        logger.info("启用代理接口...")
                        USE_PROXY_API = True
            selector = parsel.Selector(html.text)
            # 提取第一层界面快照url
            url_list = selector.xpath("//a[contains(@class,'kuaizhao')]/@href").extract()
            if url_list:
                have_data = True
                if len(url_list) < ONE_PAGE_URL:
                    cursor = len(url_list)
                else:
                    cursor = ONE_PAGE_URL
                store_urls = os.path.join(store_path, company_name + ".txt")
                with open(store_urls, 'a+', encoding='utf-8') as f1:
                    for url in url_list[:cursor]:
                        new_url = url.replace('\n', '').replace(' ', '') + '&fast=y'
                        # 写入数据
                        logger.info("保存url: %s" % new_url[:50])
                        f1.write(new_url + '\n')
                try:
                    shutil.move(store_urls, dest_path)
                except Exception as e:
                    if "already" in str(e):
                        try:
                            os.remove(store_urls)
                        except Exception as e1:
                            pass
                    else:
                        logger.warning(e)
                break
            else:
                if "抱歉没有找到" not in html.text:
                    if "百度快照" not in html.text:
                        logger.info("无搜索结果：%s" % company_name)
                        store_company(WORK_DIR, NO_COMPANIES, company_name, t_lock)
                        if USE_PROXY_API:
                            back_proxy(proxy.split('//')[-1], 200, t_lock)
                        have_data = False
                        break
                    if error_count > 1:
                        error_count -= 1
                        continue
                    else:
                        logger.info("抓取失败: %s" % company_name)
                        store_company(WORK_DIR, FAIL_COMPANIES, company_name, t_lock)
                        have_data = False
                        if not USE_PROXY_API:
                            logger.info("启用代理接口...")
                            USE_PROXY_API = True
                else:
                    logger.info("无该公司：%s" % company_name)
                    store_company(WORK_DIR, NO_COMPANIES, company_name, t_lock)
                    if USE_PROXY_API:
                        back_proxy(proxy.split('//')[-1], 200, t_lock)
                    have_data = False
                    break
        if "baiducontent" not in url:
            headers['Referer'] = url
    if have_data:
        t_lock.acquire()
        item['crawl_count'] += 1
        t_lock.release()


def store_company(work_dir, filename, company_name, t_lock):
    t_lock.acquire()
    with open(os.path.join(work_dir, filename), 'a+',
              encoding='utf-8') as f:
        f.write(company_name + '\n')
    t_lock.release()


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


# 回收代理
def back_proxy(proxy, status_code, t_lock):
    store_path = os.path.join(WORK_DIR, "proxy")
    # 判断文件是否存在，不存在则创建
    if not os.path.exists(store_path):
        os.makedirs(store_path)
    if status_code > 99 and status_code < 401 and status_code not in [301, 302]:
        # 回收
        t_lock.acquire()
        with open(os.path.join(WORK_DIR, "back_proxies.txt"), 'a+', encoding='utf-8') as f:
            f.write(proxy + '\n')
        t_lock.release()
    elif status_code in [302, 301]:
        # 回收，待定
        t_lock.acquire()
        with open(os.path.join(store_path, "back_proxies_302.txt"), 'a+', encoding='utf-8') as f1:
            f1.write(proxy + '\n')
        t_lock.release()


def main(company_list, p_queue):
    global USE_PROXY_API
    crawl_page = 1
    item = {"crawl_count": 0}
    company_list = copy(company_list)

    t_lock = tRLock()

    while True:
        # 线程池
        t_pool = tPool(THREAD_COUNT)
        if company_list:
            if len(company_list) <= THREAD_P_WAIT_COUNT:
                company_count = len(company_list)
            else:
                company_count = THREAD_P_WAIT_COUNT
            if USE_PROXY_API:
                # 根据要抓取的公司数从代理接口获取等量ip
                ip_list = get_api_proxy(company_count, PROXIES_API)
                if not ip_list or len(ip_list) < company_count:
                    logger.warning("代理接口没有ip或IP不足！！")
                    logger.info("启用本地ip...")
                    USE_PROXY_API = False
                    if ip_list:
                        for ip in ip_list:
                            back_proxy(ip, 200, t_lock)
                    continue
            else:
                # 根据要抓取的公司数从本地获取等量ip
                ip_list = get_local_proxy(company_count, LOCAL_PROXY_PATH)
                if not ip_list or len(ip_list) < company_count:
                    logger.warning("本地文件没有ip或ip不足！！")
                    time.sleep(2)
                    logger.info("尝试接口ip...")
                    USE_PROXY_API = True
                    continue
            for i in range(company_count):
                try:
                    new_company = company_list.pop().strip()
                except Exception as e:
                    logger.info(e)
                    continue
                if rds_db.sadd(RDS_COMPANIES, new_company):
                    # 线程池
                    try:
                        t_pool.apply_async(func=crawl_pages, args=(new_company, crawl_page, t_lock, item, ip_list.pop()))
                    except Exception as e:
                        logger.warning(e)
                else:
                    if USE_PROXY_API:
                        back_proxy(ip_list.pop(), 200, t_lock)
            t_pool.close()
            t_pool.join()
        else:
            break
    p_queue.put(item['crawl_count'])


def monitor(p_queue):
    total = rds_db.scard(RDS_COMPANIES)
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
    cursor = 0

    p_queue = Manager().Queue()

    # 监控爬取公司数量
    t = Thread(target=monitor, args=(p_queue,))
    t.start()

    # while True:
    #     cursor = 0
    #     # 本地文件数据
    #     with open(os.path.join(WORK_DIR, "测试1.txt"), 'r+', encoding='utf-8', errors='ignore') as f:
    #         company_list = f.readlines()
    #     if company_list:
    #         company_counts = len(company_list)
    #         try:
    #             while_1 = 0
    #             while True:
    #                 # 进程池
    #                 p_pool = pPool(PROCESS_COUNT)
    #                 for i in range(PROCESS_P_WAIT_COUNT):
    #                     if company_counts - cursor >= THREAD_P_WAIT_COUNT:
    #                         companies = company_list[cursor:cursor + THREAD_P_WAIT_COUNT]
    #                         p_pool.apply_async(func=main, args=(companies, p_queue))
    #                         cursor += THREAD_P_WAIT_COUNT
    #                     else:
    #                         companies = company_list[cursor:]
    #                         p_pool.apply_async(func=main, args=(companies, p_queue))
    #                         while_1 = 1
    #                         break
    #                 p_pool.close()
    #                 p_pool.join()
    #                 if while_1:
    #                     break
    #         except Exception as e:
    #             logger.warning(e)
    #             break

    # 接口数据
    data_url = "http://192.168.21.33:19980/tyc/list_only?number=%s"
    error_count = 0
    while True:
        try:
            # 进程池
            p_pool = pPool(PROCESS_COUNT)
            companies_list = []
            for i in range(PROCESS_P_WAIT_COUNT):
                datas = requests.get(data_url % THREAD_P_WAIT_COUNT).json()
                if datas['code'] != 200:
                    logger.info(datas)
                    error_count += 1
                    continue
                if datas['data']:
                    error_count = 0
                    for company in datas['data']:
                        companies_list.append(company['company_name'])
                    p_pool.apply_async(func=main, args=(companies_list, p_queue))
                else:
                    logger.warning("未从接口提取到公司名，请检查接口是否可用.")
                    error_count += 1
                    if error_count >= 50:
                        send_email("程序因未从接口获取到数据而被迫终止", 2)
                        logger.error("接口无数据，退出程序.")
                        break
            p_pool.close()
            p_pool.join()
            if error_count >= 50:
                break
        except Exception as e:
            logger.warning(e)
            break

    # 结束信号
    p_queue.put('end')
    t.join()

    minute, second = divmod((time.time() - start_time), 60)
    logger.info("\n总耗时: %s分 %s秒\n数据采集完成!!" % (minute, round(second, 2)), "-" * 50, sep='\n')
