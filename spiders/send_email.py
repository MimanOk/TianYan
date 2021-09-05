# -*- coding: utf-8 -*-
# : Time    : 2020/03/28
# : Author  : Miman

import time
import smtplib
from email.mime import multipart, text


def send_email(content='', level=1, retry=5):
    """
    发送邮件
    :param content: 发送内容
    :param level: 错误等级，越小等级越高，最多等级3
    :retry 失败尝试次数
    :return
    """

    if not content:
        return 1
    if isinstance(level, (str,)):
        if '-' in level:
            level = 0
        elif len(level) > 1:
            level = 2
        else:
            level = ord(level) - 48
    if isinstance(retry, (str,)):
        if '-' in retry:
            retry = 0
        elif len(retry) > 1:
            retry = 2
        else:
            retry = ord(retry) - 48
    if level < 0:
        level = 0
    elif level > 2:
        level = 2
    subject = ["小红秘书飞奔而来", "小青秘书微笑而来", "小紫秘书悄悄跟您说"][level]
    # SMTP_SERVER = "smtp.qq.com"
    # sender = 'mimanok@foxmail.com'
    # password = "tqegbjvnkcwwecfj"
    # receivers = ['mimanok@qq.com']
    # try:
    #     smtpObj = smtplib.SMTP(local_hostname='localhost', port=25)
    #     smtpObj.connect(SMTP_SERVER, '25')
    #     smtpObj.login(sender, password)
    #     smtpObj.sendmail(sender, receivers, content)
    #     print("->> Successfully sent email")
    # except smtplib.SMTPException:
    #     print("->> unable to send email")
    #     if retry:
    #         retry -= 1
    #         time.sleep(2)
    #         return send_email(content=content, level=level, retry=retry)
    #     else:
    #         return 0

    # 发送邮箱smtp服务器地址
    SMTP_SERVER = "smtp.qq.com"
    # 发送邮箱账户
    sender = "huoyijun@qq.com"
    password = "wffbacbguejwbcea"
    # 收件人地址
    receivers = "mimanok@qq.com"

    msg = multipart.MIMEMultipart()
    msg['from'] = sender
    msg['to'] = receivers
    msg['subject'] = subject
    txt = text.MIMEText(content)
    msg.attach(txt)

    try:
        smtpObj = smtplib.SMTP(local_hostname='localhost', port=25)
        smtpObj.connect(SMTP_SERVER, '25')
        smtpObj.login(sender, password)
        smtpObj.sendmail(sender, receivers, str(msg))
        print(">> Successfully sent email")
    except smtplib.SMTPException:
        print(">> unable to send email")
        if retry:
            retry -= 1
            time.sleep(2)
            return send_email(content=content, level=level, retry=retry)
        else:
            return 0
    smtpObj.quit()
    return 1


if __name__ == '__main__':
    send_email(content="陈陈", level=1, retry=3)
