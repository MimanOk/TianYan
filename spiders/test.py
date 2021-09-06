# -*- coding: utf-8 -*-
# Time: 2021.08.22
__author__ = "huoyijun"

import re
import os
import redis
import random
from loguru import logger

WORK_DIR = os.getcwd()

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


def safedecode(data, charset='utf-8', sep='#'):
    if not data: return ''
    exp = rb'[\w`~!@#$%\^&*()+-={}|\[\]\\:";\'<>?,./\r\n ]'
    word = b''
    ele_index = 0
    word_index = 0
    result = []
    while True:
        # 取元素
        try:
            ele = data[ele_index:ele_index + 1]
        except Exception as e:
            ele = b''

        # 是否存在该元素
        if ele:
            if word_index == 0:
                if not re.compile(exp).search(ele):
                    word += ele
                    word_index += 1
                else:
                    try:
                        # 正常英文数字解码
                        result.append(ele.decode(charset))
                    except Exception as e:
                        result.append(sep)
                        logger.warning(e)
                    finally:
                        word = b''
                        word_index = 0
            elif word_index == 1:
                if not re.compile(exp).search(ele):
                    word += ele
                    if "utf" not in charset:
                        try:
                            result.append(word.decode(charset))
                        except Exception as e:
                            result.append(sep * 2)
                            # logger.info("%s >> %s" % (e, str(word)))
                        word = b''
                        word_index = 0
                    else:
                        word_index += 1
                else:
                    try:
                        result.append(sep)
                        result.append(ele.decode(charset))
                    except Exception as e:
                        result.append(sep)
                        logger.warning(e)
                    finally:
                        word = b''
                        word_index = 0
            elif word_index == 2:
                if not re.compile(exp).search(ele):
                    word += ele
                    try:
                        result.append(word.decode(charset))
                    except Exception as e:
                        result.append(sep * 3)
                        # logger.info("%s >> %s" % (e, str(word)))
                else:
                    try:
                        result.append(sep * 2)
                        result.append(ele.decode(charset))
                    except Exception as e:
                        result.append(sep)
                        logger.warning(e)
                word = b''
                word_index = 0
            ele_index += 1
        else:
            break

    return ''.join(result)


# 获取本地代理
def get_local_proxy(path=None):
    if not path:
        path = os.path.join(WORK_DIR, "local_proxies.txt")  # 默认本地代理路径
    elif not os.path.exists(path):  # 判断文件是否存在，不存在则返回1
        logger.warning("proxies file '%s' not exists !" % path)
        return []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            proxies = f.readlines()
    except Exception as e:
        logger.warning(e)
        return []
    if proxies:
        for proxy in proxies:
            proxy = re.compile(r'\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{1,5}').search(proxy)
            if proxy:
                try:
                    rds_db.rpush("proxies", proxy.group(0).strip())
                except Exception as e:
                    logger.info(e)
    else:
        logger.warning("local proxy is empty !    '%s'" % path)
        return []


if __name__ == '__main__':
    get_local_proxy()
