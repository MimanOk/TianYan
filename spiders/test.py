# -*- coding: utf-8 -*-
# Time: 2021.08.22
__author__ = "huoyijun"

import re
import os
from loguru import logger


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


with open("C:\\Users\\admin\\Desktop\\zhuruidong\\virtualenv\\HuangYe\\crawl_huangye\\BackCompany\\上海启谷网络科技有限公司.txt",
          'rb') as f:
    res = f.read()
    print(res)
