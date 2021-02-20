# -*- coding: utf-8 -*-
import datetime
import pymysql
import random

from apscheduler.schedulers.blocking import BlockingScheduler


def brush_mysql():
    # 连接database
    conn = pymysql.connect(host='xxx', port=3306, user='xxx', password='xxx', database='xxx', charset='utf8')
    # 得到一个可以执行SQL语句并且将结果作为字典返回的游标
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    tables = ['artivle', 'video']
    print('到点了，开始刷数据！')
    for item in tables:
        column = 'read_num'
        if item == 'video':
            column = 'watch_num'
        # 查询数据的SQL语句
        sql_select = 'select id from {} where {} < 1000'.format(item, column)
        try:
            # 执行SQL语句
            execute = cursor.execute(sql_select)
            # 获取查询的所有记录
            results = cursor.fetchall()
            # 提交事务
            conn.commit()
            for results_item in results:
                count = random.randint(1, 5)
                # 修改数据的SQL语句
                sql_update = 'update {} set {} = {} + {} where publish = 1 and id = {}'.format(item, column, column, count, results_item['id'])
                # 执行SQL语句
                execute = cursor.execute(sql_update)
                # 提交事务
                conn.commit()
                print('{}-{}表更新数据，id为：{}，阅读量加：{}！'.format(
                    datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), item, results_item['id'], count))
            print('{}-{}表更新数据条数：{}！'.format(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), item, len(results)))
        except Exception as e:
            # 有异常，回滚事务
            conn.rollback()
        print('-' * 100)
    # 关闭光标对象
    cursor.close()
    # 关闭数据库连接
    conn.close()


scheduler = BlockingScheduler()
scheduler.add_job(brush_mysql, 'cron', second='0', minute='0', hour='1')
print('python刷数据脚本启动！')
scheduler.start()
