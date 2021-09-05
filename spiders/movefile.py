# -*- coding: utf-8 -*-
# Time: 2021.08.26
__author__ = "huoyijun"

import os
import time
import shutil
from loguru import logger


def run(cur_dir, dest_dir):
    files = os.listdir(cur_dir)
    if files:
        for file in files:
            try:
                shutil.move(os.path.join(cur_dir, file), dest_dir)
            except Exception as e:
                if "already" in str(e):
                    try:
                        os.remove(os.path.join(cur_dir, file))
                    except Exception as e1:
                        logger.info(e1)
                    continue
                else:
                    logger.info(e)
                    continue

    return len(files)


if __name__ == '__main__':
    while True:
        cur_dir = input("cur_dir: ")
        if not os.path.isdir(cur_dir):
            logger.info("无该文件夹: %s" % cur_dir)
        else: break

    dest_dir = input("dest_dir: ")
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    end_time = time.time() - 120
    while True:
        start_time = time.time()
        if start_time - end_time >= 120:
            filecount = run(cur_dir, dest_dir)
            minute, second = divmod((time.time() - start_time), 60)
            logger.info("\n总耗时: %s分 %s秒\n移动文件数: %s" % (minute, round(second, 2), filecount))
            end_time = time.time()
        time.sleep(1)
