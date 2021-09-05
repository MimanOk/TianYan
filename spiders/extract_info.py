# -*- coding: utf-8 -*-
# Time: 2021.08.22
__author__ = "huoyijun"

import json
import os
import re
import time
import shutil
import redis
import requests
import parsel
from loguru import logger
import multiprocessing
from threading import Thread
from send_email import send_email
from threading import RLock as tRLock
# from twisted.enterprise import adbapi
from collections.abc import Iterable
# from DBUtils.PooledDB import PooledDB
from multiprocessing.dummy import Pool as tPool
from multiprocessing import Manager
from multiprocessing import Pool as pPool


# 线程数 32
THREAD_COUNT = 32
# 进程数 2
PROCESS_COUNT = 2
# 线程池缓冲数(线程流畅度) 4
THREAD_P_WAIT_COUNT = THREAD_COUNT * 4
# 进程池缓冲数(进程流畅度) 64
PROCESS_P_WAIT_COUNT = PROCESS_COUNT * 64

# 数据库接口
url_api = "http://192.168.21.33:19980/boss/base?insert=ignore"
# 请求头
HEADERS = {
    'Content-Type': 'application/json'
}

# 工作目录
WORK_DIR = os.getcwd()
trace30 = logger.add(os.path.join(WORK_DIR, "logs", "extract_info.log"), rotation="00:00", level=30)
# 保存路径
store_path = os.path.join(WORK_DIR, "CompanyDetail")
from_path = os.path.join(WORK_DIR, "CompanyInfo")
back_path = os.path.join(WORK_DIR, "BackCompany")
if not os.path.exists(store_path):
    os.makedirs(store_path)
if not os.path.exists(from_path):
    os.makedirs(from_path)
if not os.path.exists(back_path):
    os.makedirs(back_path)


# class ThreadInsert(object):
#     """ 多线程并发MySQL插入数据 """
#     def __init__(self, max_conn):
#         self.max_conn = max_conn
#         self.pool = self.mysql_connection()
#
#     def mysql_connection(self):
#         pool = PooledDB(
#             pymysql,
#             self.max_conn,
#             host='localhost',
#             user='root',
#             port=3306,
#             passwd='123456',
#             db='test_DB',
#             use_unicode=True)
#         return pool
#
#     def mysql_insert(self, *args):
#         con = self.pool.connection()
#         cur = con.cursor()
#         sql = "INSERT INTO test(sku, fnsku, asin, shopid) VALUES(%s, %s, %s, %s)"
#         try:
#             cur.executemany(sql, *args)
#             con.commit()
#         except Exception as e:
#             con.rollback()  # 事务回滚
#             print('SQL执行有误,原因:', e)
#         finally:
#             cur.close()
#             con.close()


def parse_file(file, item, t_lock):
    with open(file, 'rb') as f:
        _data = f.read()
        # pages = re.compile(rb'<!DOCTYPE HTML>.*?</html>', re.DOTALL).findall(_data)
        pages = [_data]
        if pages:
            for page in pages:
                try:
                    charset = re.compile(b'charset=["]?(.*?)"', re.DOTALL).search(page)
                    if charset:
                        try:
                            charset = charset.group(1).decode('utf8')
                        except Exception as e:
                            logger.warning("网页编码无法解码: %s >> %s" % (e, str(charset)))
                            continue

                        # 公司名称
                        company_name = re.compile(r'<h1 class="name"(.*?)/h1>'.encode(charset), re.DOTALL).search(page)
                        # 经营范围(主营产品)
                        main_product = re.compile(r'class="select-none"(.*?)/span>'.encode(charset), re.DOTALL).search(page)
                        # 统一社会信用代码
                        code = re.compile(r'统一社会信用代码.*?</td>.{,2}<td>(.*?)</td>'.encode(charset)).search(page)
                        # 公司简介
                        company_profile = re.compile(r'简介：{,2}</span(.*?)span'.encode(charset), re.DOTALL).search(page)
                        # 股东名字
                        # shareholders_name = re.compile(r'class="data-title">股东信息.*?class="table-toco.*?<td onclick=".*?<a.*?title="(.*?)"'.encode(charset), re.DOTALL).findall(page)
                        str_page = safedecode(page, charset)
                        selector = parsel.Selector(str_page)
                        shareholders_name = selector.css(r'#_container_holderCount .table-toco td[onclick] a::attr(title)').extract()

                        # 二次提取公司名称
                        if company_name:
                            company_name = company_name.group(1)
                            if re.compile(rb'>.*?<').search(company_name):
                                company_name_list = re.compile(rb'>(.*?)<', re.DOTALL).findall(company_name)
                                if company_name_list:
                                    values = []
                                    for name in company_name_list:
                                        if name:
                                            try:
                                                values.append(
                                                    safedecode(name.replace(b' ', b'').replace(b'\r\n', b'').replace(b'\n', b''), charset))
                                            except Exception as e:
                                                logger.warning("%s >> %s" % (e, name))
                                    company_name = ''.join(values)
                                    values.clear()
                            else:
                                company_name = safedecode(company_name.replace(b' ', b'').replace(b'\r\n', b'').replace(b'\n', b''), charset)
                        else:
                            company_name = ''

                        # 二次提取主营产品
                        if main_product:
                            main_product = main_product.group(1)
                            if re.compile(rb'>.*?<').search(main_product):
                                main_product_list = re.compile(rb'>(.*?)<', re.DOTALL).findall(main_product)
                                if main_product_list:
                                    values = []
                                    for pro in main_product_list:
                                        if pro:
                                            try:
                                                values.append(
                                                    safedecode(pro.replace(b' ', b'').replace(b'\r\n', b'').replace(b'\n', b''), charset))
                                            except Exception as e:
                                                logger.warning("%s >> %s" % (e, pro))
                                    main_product = ''.join(values)
                                    values.clear()
                            else:
                                main_product = safedecode(main_product.replace(b' ', b'').replace(b'\r\n', b'').replace(b'\n', b''), charset)
                        else:
                            main_product = ''

                        # 二次提取统一社会信用代码
                        if code:
                            code = code.group(1).decode(charset)
                        else:
                            code = ''

                        # 二次提公司简介
                        if company_profile:
                            company_profile = company_profile.group(1)
                            if re.compile(rb'>.*?<').search(company_profile):
                                company_profile_list = re.compile(rb'>(.*?)<', re.DOTALL).findall(company_profile)
                                if company_profile_list:
                                    values = []
                                    for pro_ in company_profile_list:
                                        if pro_:
                                            try:
                                                values.append(
                                                    safedecode(pro_.replace(b' ', b'').replace(b'\r\n', b'').replace(b'\n', b''), charset))
                                            except Exception as e:
                                                logger.warning("%s >> %s" % (e, pro_))
                                    company_profile = ''.join(values)
                                    values.clear()
                            else:
                                company_profile = safedecode(company_profile.replace(b' ', b'').replace(b'\r\n', b'').replace(b'\n', b''), charset)
                        else:
                            company_profile = ''

                        if shareholders_name:
                            shareholders_name = ';'.join(shareholders_name)
                        else:
                            shareholders_name = re.compile(r'class="label">法定代表人：.{,2}</span>.{,2}<span>.{,2}<a title="(.*?)"'.encode(charset)).search(page)
                            if shareholders_name:
                                shareholders_name = shareholders_name.group(1).decode(charset)
                            else:
                                shareholders_name = ''

                        if company_name:
                            # if rds_db.sadd("parse_company", company_name):
                            main_product = main_product.replace('&rdquo;', '').replace('&nbsp;', ' ').replace(
                                '&ldquo;', '')
                            company_profile = company_profile.replace('&rdquo;', '').replace('&nbsp;', ' ').replace(
                                '&ldquo;', '') + " 主营产品：" + main_product

                            data = "\n公司名:%s\n统一社会信用代码:%s\n公司简介:%s\n股东:%s" % (company_name, code, company_profile, shareholders_name)
                            logger.info(data)
                            # if "#" in company_name:
                            #     logger.info(file, page)

                            # >>>unlock
                            # 数据解析到本地
                            t_lock.acquire()
                            with open(os.path.join(store_path, company_name + '.txt'), 'w+', encoding='utf8') as f:
                                f.write(data)
                            t_lock.release()
                            logger.info("%s   待入库" % company_name)

                            # 数据解析到数据库
                            formdata = {
                                "company_name": company_name,
                                "social_credit_id": code,
                                "company_about": company_profile
                            }

                            t_lock.acquire()
                            item[1].append(formdata)
                            item[0] += 1
                            t_lock.release()
                    else:
                        logger.info("网页无字符编码")

                except Exception as e:
                    logger.warning("未知错误: %s" % e)
                    continue


def parse_files(file_list, p_queue):
    if not isinstance(file_list, (Iterable,)) or not file_list:
        logger.warning("file list error")
        return None
    # 0:总解析页数，1:要插入的数据
    item = [0, []]

    # 线程锁
    t_lock = tRLock()
    # 线程池
    t_pool = tPool(THREAD_COUNT)

    for file in file_list:
        new_file = file.strip()
        # 线程池
        if new_file and os.path.isfile(new_file):
            try:
                t_pool.apply_async(func=parse_file, args=(new_file, item, t_lock))
            except Exception as e:
                logger.warning(e)
    t_pool.close()
    t_pool.join()

    # >>>lock
    # 数据入库
    # logger.info("数据入库中")
    # res = requests.post(url=url_api, data=json.dumps(item[1]), headers=HEADERS).json()
    # if res['code'] != 200:
    #     # 保存失败数据
    #     for comapny in json.loads(item[1]):
    #         company_name = comapny['company_name']
    #         t_lock.acquire()
    #         with open(os.path.join(os.path.dirname(WORK_DIR), 'insert_fail_companies.txt'), 'a+',
    #                   encoding='utf8') as f:
    #             f.write(company_name + '\n')
    #         t_lock.release()
    #         logger.warning("%s数据插入失败，已保存至本地" % company_name)
    #     item[1].clear()
    #     return None
    # item[1].clear()
    # try:
    #     logger.info("试存数: %s, 成功数: %s" % (item[0], res['rows_count']))
    # except Exception as e:
    #     logger.warning(e)
    p_queue.put(item[0])


    for back_file in file_list:
        try:
            shutil.move(back_file, back_path)
        except Exception as e:
            if "already" in str(e):
                try:
                    os.remove(back_file)
                except Exception as e1:
                    pass
                continue
            else:
                logger.warning(e)
                continue


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


def monitor(p_queue):
    total = 0
    while True:
        try:
            temp = p_queue.get()
        except Exception as e:
            temp = ''
            logger.info("监控线程休眠...")
        if temp == 'end':
            logger.info("监控到总解析数据量: %s" % total)
            break
        if temp:
            total += temp
            logger.info("当前解析数量: %s" % total)


if __name__ == '__main__':
    start_time = time.time()
    multiprocessing.freeze_support()

    p_lock = Manager().RLock()
    p_queue = Manager().Queue()

    # 监控系统
    t = Thread(target=monitor, args=(p_queue,))
    t.start()

    while True:
        files = os.listdir(from_path)
        cursor = 0
        if files:
            file_count = len(files)
            try:
                while_1 = 0
                while True:
                    # 进程池
                    p_pool = pPool(PROCESS_COUNT)
                    for i in range(PROCESS_P_WAIT_COUNT):
                        if file_count - cursor >= THREAD_P_WAIT_COUNT:
                            new_files = [os.path.join(from_path, file) for file in
                                         files[cursor:cursor + THREAD_P_WAIT_COUNT]]
                            p_pool.apply_async(func=parse_files, args=(new_files, p_queue))
                            cursor += THREAD_P_WAIT_COUNT
                        else:
                            new_files = [os.path.join(from_path, file) for file in files[cursor:]]
                            p_pool.apply_async(func=parse_files, args=(new_files, p_queue))
                            while_1 = 1
                            break
                    p_pool.close()
                    p_pool.join()
                    if while_1:
                        break
            except Exception as e:
                logger.warning(e)
                send_email("extract_info 解析有误", 2, 5)
                break
        else:
            logger.info("无文件")
            time.sleep(5)
    p_queue.put('end')
    t.join()
    end_time = time.time()
    minute, second = divmod((end_time - start_time), 60)
    logger.info("\n总耗时: %s 分 : %s 秒\n完成!!" % (minute, round(second, 2)), "-" * 50, sep='\n')
