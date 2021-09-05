# -*- coding: utf-8 -*-
# Time: 2021.09.02
__author__ = "huoyijun"

import os
import time
from loguru import logger


def run(cur_dir):
    files = os.listdir(cur_dir)
    if files:
        for file in files:
            try:
                os.remove(os.path.join(cur_dir, file))
            except Exception as e:
                logger.info(e)
                continue
    return len(files)


if __name__ == '__main__':
    while True:
        cur_dir = input("cur_dir: ")
        if not os.path.isdir(cur_dir):
            logger.info("无该文件夹: %s" % cur_dir)
        else: break

    end_time = time.time() - 120
    while True:
        start_time = time.time()
        if start_time - end_time >= 120:
            filecount = run(cur_dir)
            minute, second = divmod((time.time() - start_time), 60)
            logger.info("\n总耗时: %s分 %s秒\n删除文件数: %s" % (minute, round(second, 2), filecount))
            end_time = time.time()
        time.sleep(1)
