# -*- coding: utf-8 -*-
# Time: 2021.09.05
__author__ = "huoyijun"

import os
import re
import time
import random
import requests
from loguru import logger
from threading import RLock as tRLock
from multiprocessing.dummy import Pool as tPool


# 线程数(同时消耗每个文件中的url数) 24
THREAD_COUNT = 24
# 进程数(一次使用的文件数) 2
PROCESS_COUNT = 2
# 线程池缓冲数(线程流畅度) 10
THREAD_P_WAIT_COUNT = THREAD_COUNT * 10
# 进程池缓冲数(进程流畅度) 32
PROCESS_P_WAIT_COUNT = PROCESS_COUNT * 32

# 代理测试网址
# check_url = "https://httpbin.org/get"
check_url = "https://www.baidu.com"
# 代理测试间隔时间
interval_time = 300
WORK_DIR = os.getcwd()
from_path = os.path.join(WORK_DIR, "local_proxies.txt")
store_path = os.path.join(WORK_DIR, "valid_proxies.txt")

ua_list = []
with open(os.path.join(os.getcwd(), "ua.txt"), 'r+', encoding='utf-8') as ua_f:
    ua_list_ = ua_f.readlines()
    for ua in ua_list_:
        if isinstance(ua, (bytes,)):
            ua_list.append(ua.decode().strip())
        else:
            ua_list.append(ua.strip())

headers = {
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh,zh-TW;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6',
    # 'Cache-Control': 'max-age=0',
    # 'Connection': 'keep-alive',
    # 'Host': 'httpbin.org',
    # 'Upgrade-Insecure-Requests': '1',
    'User-Agent': random.choice(ua_list),
}


def check_proxy(url, proxy, tlock, f, item):
    try:
        proxy = re.compile(r'\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{1,5}').search(proxy)
        if proxy:
            proxy = proxy.group(0)
            try:
                _proxy = {"http": "http://" + proxy, "https": "https://" + proxy}
                res = requests.get(url, headers=headers, proxies=_proxy, timeout=8)
                status_code = res.status_code
            except Exception as e:
                logger.info(e.args)
                return None

        if status_code > 99 and status_code < 401 and status_code not in [301, 302]:
            # 回收
            logger.info("valid proxy %s" % proxy)
            tlock.acquire()
            f.write(proxy + '\n')
            item[0] += 1
            tlock.release()
        elif status_code in [302, 301]:
            # 回收，待定
            with open(os.path.join(store_path, "302.txt"), 'a+', encoding='utf8') as f1:
                tlock.acquire()
                f1.write(proxy + '\n')
                item[0] += 1
                tlock.release()
    except Exception as e:
        if "HTTP" in str(e.args):
            logger.info("invalid proxy %s" % proxy)
        else:
            logger.info(e)


if __name__ == '__main__':
    tlock = tRLock()
    item = [0]
    end_time = time.time() - interval_time
    while True:
        start_time = time.time()
        if start_time - end_time >= interval_time:
            if os.path.isfile(from_path):
                with open(from_path, 'r+', encoding='utf8') as f:
                    with open(store_path, 'a+', encoding='utf8') as f1:
                        _proxies = f.readlines()
                        if _proxies:
                            tpool = tPool(THREAD_COUNT)
                            for proxy in _proxies:
                                tpool.apply_async(func=check_proxy, args=(check_url, proxy, tlock, f1, item))
                            tpool.close()
                            tpool.join()
            minute, second = divmod((time.time() - start_time), 60)
            logger.info("\n总耗时: %s分 %s秒\n测试代理数量: %s" % (minute, round(second, 2), item[0]))
            item[0] = 0
            end_time = time.time()
        time.sleep(1)
