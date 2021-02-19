# -*- coding: utf-8 -*-
import hashlib


def my_md5(s, salt=''):
    # 加盐，盐的默认值是空
    s = s+salt
    # 先变成bytes类型才能加密
    news = str(s).encode()
    # 创建md5对象
    m = hashlib.md5(news)
    return m.hexdigest()